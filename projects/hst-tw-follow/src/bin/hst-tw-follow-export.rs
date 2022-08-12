//use chrono::Utc;
//use hst_tw_follow::db::update_user_relations;
//use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /*let pool = PgPoolOptions::new()
    .max_connections(5)
    .connect("postgres://travis@localhost/twitter-follow")
    .await?;*/

    let args = std::env::args().collect::<Vec<_>>();

    /*for batch in hst_tw_follow::file::read_batches(&args[1]) {
        let batch = batch?;
        println!(
            "{}, {}, {}",
            batch.user_id,
            batch.timestamp,
            batch.follower_change.addition_ids.len()
        );
    }*/

    let mut writer = std::io::BufWriter::new(std::fs::File::create("out.dat")?);

    hst_tw_follow::archive::write_batches(
        &mut writer,
        hst_tw_follow::file::read_batches(&args[1]),
    )?;

    /*let user_id = 123;
    let timestamp = Utc::now();
    let follower_ids = vec![1, 2, 3, 4, 6].into_iter().collect();
    let followed_ids = vec![10, 11, 12, 13, 15].into_iter().collect();

    let mut connection = pool.acquire().await?;

    update_user_relations(
        &mut connection,
        user_id,
        timestamp,
        follower_ids,
        followed_ids,
    )
    .await?;*/

    Ok(())
}
