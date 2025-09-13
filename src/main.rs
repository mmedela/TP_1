use std::{
    collections::HashMap, 
    fs::File, io::{BufReader, Read}, 
    sync::{mpsc::{self, Receiver, Sender}, Arc, Mutex}, 
    thread::{self, JoinHandle}, 
    time::Instant, u8
};

const CHUNK_SIZE: usize = 1024 * 1024;
const NUM_WORKERS: usize = 4;
const LINE_JUMP_AS_BYTES: u8 = 10;

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

fn process_file(file: File, tx: Sender<Vec<u8>>)->JoinHandle<()>{
  
    let producer_handle: JoinHandle<()> = thread::spawn(move || {
        let mut reader: BufReader<File> = BufReader::new(file);
        
        let mut buffer: Vec<u8> = vec![0; CHUNK_SIZE];
        let mut incomplete_line:Vec<u8> = Vec::new();

        loop{
            let n = reader.read(&mut buffer).unwrap();
            if n == 0{
                break;
            }
            for i in (0..n).rev() {
                if buffer[i] != LINE_JUMP_AS_BYTES {
                    continue;
                }
                tx.send([&incomplete_line[..], &buffer[..i]].concat()).expect("Failed to send chunk");
                incomplete_line.clear();
                incomplete_line.extend_from_slice(&buffer[i..n]);
                break;
            }
        }
        if !buffer.is_empty() {
            tx.send(buffer).expect("Failed to send final chunk");
        }
    });

    return producer_handle;
}

fn main() -> std::io::Result<()>{
    let start = Instant::now();
    let file = File::open("measurements.txt")?;
    let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::channel();

    let producer_handle: JoinHandle<()> = process_file(file, tx);

    let mut consumers_handles:Vec<JoinHandle<HashMap<String, (f64, f64, f64, usize)>>> = Vec::new();
    let rx: Arc<Mutex<Receiver<Vec<u8>>>> = Arc::new(Mutex::new(rx));

    for _ in 0..NUM_WORKERS {

        let reciever = Arc::clone(&rx);
        let handle: JoinHandle<HashMap<String, (f64, f64, f64, usize)>> = thread::spawn( move || {
            let mut stats: HashMap<String, (f64, f64, f64, usize)> = HashMap::new();
            while let Ok(buffer) = reciever.lock().unwrap().recv(){
                let mut start = 0;
                for end in 0..buffer.len(){
                    if buffer[end] == LINE_JUMP_AS_BYTES{
                        add_entry_to_hash(str::from_utf8(&buffer[start..end]).unwrap(), &mut stats);
                        start = end;
                    }
                }
                
            }
            return stats;
        });
        consumers_handles.push(handle);
    }

    producer_handle.join().unwrap();

    for handle in consumers_handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    println!("Tiempo de ejecución: {:?}", duration);
    Ok(())
}
