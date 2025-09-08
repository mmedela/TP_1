use std::{collections::HashMap, fs::File, io::{BufReader, Read}, time::Instant};

fn main() -> std::io::Result<()>{
    let start = Instant::now();
    let file = File::open("measurements.txt")?;
    let mut reader = BufReader::new(file);

    let mut stats: HashMap<String, (f64, f64, f64, usize)> = HashMap::new();
    let mut buffer = [0u8; 1024 * 1024];

    let mut chunk: &str;
    let mut line: &str;

    loop{
        let n = reader.read(&mut buffer)?;
        if n == 0{
            break;
        }
        let string = String::from_utf8_lossy(&buffer);
        chunk = &string;

        while let Some(pos) = chunk.find('\n') {
            if pos == 0 {
                break;
            } 
            line = &chunk[..pos - 1];
            chunk = &chunk[pos + 1..];

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

    }
    for (city, (min, max, sum, count)) in &stats {
        let avg = sum / *count as f64;
        println!("{}: min={:.1}, max={:.1}, avg={:.1}", city, min, max, avg);
    }
    let duration = start.elapsed();
    println!("Tiempo de ejecución: {:?}", duration);
    Ok(())
}
