use chrono::Utc;
use hst_tw_follow::db::update_from_batches;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://travis@localhost/twitter-follow")
        .await?;

    let args = std::env::args().collect::<Vec<_>>();
    let reader = hst_tw_follow::archive::FollowReader::new(std::io::BufReader::new(
        std::fs::File::open(&args[1])?,
    ));

    let mut connection = pool.acquire().await?;

    update_from_batches(&mut connection, reader).await?;

    /*let user_id = 123;
    let timestamp = Utc::now();
    let follower_ids = vec![1, 2, 3, 4, 6].into_iter().collect();
    let followed_ids = vec![10, 11, 12, 13, 15].into_iter().collect();

    let mut connection = pool.acquire().await?;

    update_from_full(
        &mut connection,
        timestamp,
        user_id,
        follower_ids,
        followed_ids,
    )
    .await?;*/

    Ok(())
}
