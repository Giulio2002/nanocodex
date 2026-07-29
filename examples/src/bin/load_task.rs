use std::error::Error;

use nanocodex_eval::Task;

fn main() -> Result<(), Box<dyn Error>> {
    let directory = std::env::args_os()
        .nth(1)
        .ok_or("usage: cargo run -p nanocodex-examples --bin load-task -- TASK_DIRECTORY")?;
    let task = Task::load(directory)?;

    println!("{}", task.name());
    println!("{}", task.prompt());
    Ok(())
}
