use rocket::futures::TryFutureExt;
use rocket::State;
use rocket::http::Status;
use scylla::{transport::errors::QueryError, IntoTypedRows, Session, SessionBuilder};
use std::net::Ipv4Addr;
#[macro_use]
extern crate rocket;

async fn valid_experiment(
    db: &Session,
    experiment_id: &str,
) -> Result<bool, scylla::transport::errors::QueryError> {
    let rows_opt = db
        .query(
            "select experimentid, scheme from cloverleaf.experiments",
            &[],
        )
        .await?
        .rows;
    if let Some(rows) = rows_opt {
        for row in rows.into_typed::<(String, String)>() {
            let (known_experiment_id, _): (String, String) =
                row.map_err(|_| scylla::transport::errors::BadQuery::Other("Could not select experiments from DB".to_string()))?;
            if known_experiment_id == experiment_id {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn post_result(
    db: &Session,
    experiment_id: &str,
    result: &str,
) -> Result<(), scylla::transport::errors::QueryError> {
    db.query(
        "insert into cloverleaf.results (experimentid, result) VALUES (?, ?)",
        (experiment_id, result),
    )
    .await?;
    Ok(())
}

#[post("/publish/<experiment_id>", data = "<result>")]
async fn index(experiment_id: &str, result: String, db: &State<Session>) -> (Status, String) {
    valid_experiment(db, experiment_id)
        .and_then(|valid_experiment| async move {
            if valid_experiment {
                post_result(db, experiment_id, result.as_str()).and_then(|_| async move {
                    Ok((Status::Ok, format!("Result posted for {} experiment.\n", experiment_id)))
                }).await
            } else {
                Ok((Status::NotFound, format!("'{}' is an invalid experiment identifier.\n", experiment_id)))
            }
        })
        .await.or_else(|e| {
            Ok::<_, QueryError>((Status::InternalServerError, e.to_string()))
        }).unwrap()
}

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    let scylla = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .map_err(|_| {
            rocket::error::Error::from(rocket::error::ErrorKind::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Oops",
            )))
        })
        .await?;
    let configuration = rocket::Config {
        port: 8080,
        address: std::net::IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        ..Default::default()
    };
    rocket::build()
        .configure(configuration)
        .manage(scylla)
        .mount("/", routes![index])
        .launch()
        .await?;
    Ok(())
}
