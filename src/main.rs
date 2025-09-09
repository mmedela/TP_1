use std::{
    collections::HashMap, 
    fs::File, io::{BufReader, Read}, 
    sync::mpsc::{self, Receiver, Sender}, 
    thread::{self, JoinHandle}, 
    time::Instant
};

const CHUNK_SIZE: usize = 1024 * 1024;
const NUM_WORKERS: usize = 4;

fn add_entry_to_hash(line: &str, stats: &mut HashMap<String, (f64, f64, f64, usize)>){
    if let Some((city, temp_str)) = line.split_once(';') {
        if let Ok(temp) = temp_str.parse::<f64>() {
            stats.entry(city.to_string())
                .and_modify(|e| {
                    e.0 = e.0.min(temp);
                    e.1 = e.1.max(temp);
                    e.2 += temp;        
                    e.3 += 1;           
                })
                .or_insert((temp, temp, temp, 1));
        }
    }
}

fn process_file(file: File)->JoinHandle<HashMap<String, (f64, f64, f64, usize)>>{

    let producer_handle: JoinHandle<HashMap<String, (f64, f64, f64, usize)>> = thread::spawn(move || {
        let mut reader = BufReader::new(file);
        let mut stats: HashMap<String, (f64, f64, f64, usize)> = HashMap::new();
        
        let mut buffer = [0u8; CHUNK_SIZE];

        let mut chunk: &str;
        let mut line: &str;
        loop{
            let n = reader.read(&mut buffer).unwrap();
            if n == 0{
                break;
            }
            let string = String::from_utf8_lossy(&mut buffer);
            chunk = &string;

            while let Some(pos) = chunk.find('\n') && pos != 0 {
                line = &chunk[..((pos - 1).max(0))];
                chunk = &chunk[pos + 1..];

                add_entry_to_hash(line, &mut stats);
            }
        }
        return stats
    });

    return producer_handle;
}

fn main() -> std::io::Result<()>{
    let start = Instant::now();
    let file = File::open("measurements.txt")?;

    let (tx, rx): (Sender<Vec<String>>, Receiver<Vec<String>>) = mpsc::channel();

    let producer_handle = process_file(file);
    let stats = producer_handle.join().unwrap();
    
    // let stats: HashMap<String, (f64, f64, f64, usize)> = process_file(file)?;
    for (city, (min, max, sum, count)) in &stats {
        let avg = sum / *count as f64;
        println!("{}: min={:.1}, max={:.1}, avg={:.1}", city, min, max, avg);
    }
    let duration = start.elapsed();
    println!("Tiempo de ejecución: {:?}", duration);
    Ok(())
}
