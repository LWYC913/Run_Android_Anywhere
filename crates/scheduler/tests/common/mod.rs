use sqlx::{Connection as _, PgConnection};

// The live scheduler tests share the production JetStream resource names and
// Cargo may execute their integration-test binaries concurrently. Hold a
// session lock for each test's lifetime so topology drift and durable pulls
// cannot interfere across binaries.
const SHARED_NATS_TOPOLOGY_TEST_LOCK: i64 = 0x5241_414e_4154_5334;

pub async fn acquire_shared_nats_topology_lock(
    database_url: &str,
) -> Result<PgConnection, sqlx::Error> {
    let mut connection = PgConnection::connect(database_url).await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(SHARED_NATS_TOPOLOGY_TEST_LOCK)
        .execute(&mut connection)
        .await?;
    Ok(connection)
}
