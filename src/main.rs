use tokio::fs::File;
use tokio::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut reader: &[u8] = b"hello";
    let mut f = File::create("foo.txt").await?;

    io::copy(&mut reader, &mut f).await?;

    Ok(())
}
