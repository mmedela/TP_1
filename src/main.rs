use rayon::prelude::*;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
    str,
    sync::mpsc,
    thread,
    time::Instant,
};

const CHUNK_SIZE: usize = 1024 * 1024;
const LINE_JUMP_AS_BYTES: u8 = b'\n';

fn add_entry_to_hash(
    line: &str,
    stats: &mut HashMap<String, (f32, f32, f32, usize)>,
) {
    if let Some((city, temp_str)) = line.split_once(';')
        && let Ok(temp) = temp_str.parse::<f32>() {
            stats
                .entry(city.to_string())
                .and_modify(|e| {
                    e.0 = e.0.min(temp);
                    e.1 = e.1.max(temp);
                    e.2 += temp;
                    e.3 += 1;
                })
                .or_insert((temp, temp, temp, 1));
        }
}

fn stream_chunks(file: File, tx: mpsc::Sender<Vec<u8>>) {
    let mut reader = BufReader::new(file);
    let mut buffer = vec![0; CHUNK_SIZE];
    let mut leftover = Vec::new();

    loop {
        let bytes_read = reader.read(&mut buffer).unwrap();
        if bytes_read == 0 {
            break;
        }

        let data = &buffer[..bytes_read];
        let mut split_at = None;

        for i in (0..data.len()).rev() {
            if data[i] == LINE_JUMP_AS_BYTES {
                split_at = Some(i + 1);
                break;
            }
        }

        match split_at {
            Some(index) => {
                let mut chunk = leftover.clone();
                chunk.extend_from_slice(&data[..index]);
                tx.send(chunk).unwrap();

                leftover.clear();
                leftover.extend_from_slice(&data[index..]);
            }
            None => {
                leftover.extend_from_slice(data);
            }
        }
    }

    if !leftover.is_empty() {
        tx.send(leftover).unwrap();
    }
}

fn process_chunk(buffer: Vec<u8>) -> HashMap<String, (f32, f32, f32, usize)> {
    let mut stats = HashMap::new();
    let mut start = 0;

    for i in 0..buffer.len() {
        if buffer[i] == LINE_JUMP_AS_BYTES {
            if let Ok(line) = str::from_utf8(&buffer[start..i]) {
                add_entry_to_hash(line, &mut stats);
            }
            start = i + 1;
        }
    }

    if start < buffer.len()
        && let Ok(line) = str::from_utf8(&buffer[start..]) {
            add_entry_to_hash(line, &mut stats);
        }

    stats
}

fn merge_maps(
    mut a: HashMap<String, (f32, f32, f32, usize)>,
    b: HashMap<String, (f32, f32, f32, usize)>,
) -> HashMap<String, (f32, f32, f32, usize)> {
    for (city, (min, max, sum, count)) in b {
        a.entry(city)
            .and_modify(|e| {
                e.0 = e.0.min(min);
                e.1 = e.1.max(max);
                e.2 += sum;
                e.3 += count;
            })
            .or_insert((min, max, sum, count));
    }
    a
}

fn main() -> std::io::Result<()> {
    let start = Instant::now();

    let file = File::open("measurements.txt")?;
    let (tx, rx) = mpsc::channel();

    let producer = thread::spawn(move || {
        stream_chunks(file, tx);
    });

    let final_stats = rx
        .into_iter()
        .par_bridge()
        .map(process_chunk)
        .reduce(HashMap::new, merge_maps);

    producer.join().unwrap();

    for (city, (min, max, sum, count)) in &final_stats {
        let avg = sum / *count as f32;
        println!(
            "{} -> min: {:.2}, max: {:.2}, avg: {:.2}",
            city, min, max, avg
        );
    }

    let duration = start.elapsed();
    println!("Tiempo de ejecución: {:?}", duration);
    Ok(())
}
