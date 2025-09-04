use std::{collections::HashMap, fs::File, io::{BufRead, BufReader}, time::Instant};

fn main() -> std::io::Result<()>{
    let start = Instant::now();
    let file = File::open("measurements.txt")?;
    let reader = BufReader::new(file);

    let mut stats: HashMap<String, (f64, f64, f64, usize)> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
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

    for (city, (min, max, sum, count)) in &stats {
        let avg = sum / *count as f64;
        println!("{}: min={:.1}, max={:.1}, avg={:.1}", city, min, max, avg);
    }
    let duration = start.elapsed();
    println!("Tiempo de ejecución: {:?}", duration);
    Ok(())
}
