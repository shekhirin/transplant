use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn get_settings_unexisting_index() {
    let server = Server::new().await;
    let (response, code) = server.version().await;
    assert_eq!(code, 200);
    let version = response.as_object().unwrap();
    assert!(version.get("commitSha").is_some());
    assert!(version.get("commitDate").is_some());
    assert!(version.get("pkgVersion").is_some());
}

#[actix_rt::test]
async fn test_healthyness() {
    let server = Server::new().await;

    let (response, status_code) = server.service.get("/health").await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "available");
}

#[actix_rt::test]
async fn stats() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(Some("id")).await;

    assert_eq!(code, 200);

    let (response, code) = server.stats().await;

    assert_eq!(code, 200);
    assert!(response.get("databaseSize").is_some());
    assert!(response.get("lastUpdate").is_some());
    assert!(response["indexes"].get("test").is_some());
    assert_eq!(response["indexes"]["test"]["numberOfDocuments"], 0);
    assert_eq!(response["indexes"]["test"]["isIndexing"], false);

    let last_update = response["lastUpdate"].as_str().unwrap();

    let documents = json!([
        {
            "id": 1,
            "name": "Alexey",
        },
        {
            "id": 2,
            "age": 45,
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    assert_eq!(response["updateId"], 0);

    let (response, code) = server.stats().await;

    assert_eq!(code, 200);
    assert_eq!(response["indexes"]["test"]["isIndexing"], true);

    index.wait_update_id(0).await;

    let (response, code) = server.stats().await;

    assert_eq!(code, 200);
    assert!(response["databaseSize"].as_u64().unwrap() > 0);
    assert!(response["lastUpdate"].as_str().unwrap() > last_update);
    assert_eq!(response["indexes"]["test"]["numberOfDocuments"], 2);
    assert_eq!(response["indexes"]["test"]["isIndexing"], false);
    assert_eq!(response["indexes"]["test"]["fieldsDistribution"]["id"], 2);
    assert_eq!(response["indexes"]["test"]["fieldsDistribution"]["name"], 1);
    assert_eq!(response["indexes"]["test"]["fieldsDistribution"]["age"], 1);
}
