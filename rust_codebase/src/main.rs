use csv::{ReaderBuilder, WriterBuilder};
use flate2::read::GzDecoder;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

fn process_file(path: &Path) -> HashMap<String, (u64, f64)> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => {
            eprintln!("Error: Could not open file {}", path.display());
            return HashMap::new();
        }
    };

    let gz = GzDecoder::new(file);
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b' ')
        .from_reader(gz);
    
    let mut project_views = HashMap::new();

    for result in rdr.records() {
        let record = match result {
            Ok(r) => r,
            Err(_) => {
                eprintln!("Warning: Skipping malformed record in file {}", path.display());
                continue;
            },
        };

        if record.len() < 4 {
            eprintln!("Warning: Skipping incomplete record in file {}: {:?}", path.display(), record);
            continue;
        }

        let project = &record[0];
        let views_str = &record[2];
        
        // Handle potential empty strings or non-numeric view counts
        let views: u64 = match views_str.trim().parse() {
            Ok(v) => v,
            Err(_) => {
                eprintln!("Warning: Skipping invalid view count in file {}: {}", path.display(), views_str);
                continue;
            },
        };

        let processed_views = (views as f64).powi(2);
        let entry = project_views.entry(project.to_string()).or_insert((0, 0.0));
        entry.0 += views;
        entry.1 += processed_views;
    }

    project_views
}

fn main() {
    let directory = "../dataset";  // Dataset path
    let output_file = "../output/rust_result.csv";  // Output file path
    let paths: Vec<std::path::PathBuf> = match std::fs::read_dir(directory) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().map(|e| e == "gz").unwrap_or(false))
            .map(|entry| entry.path())
            .collect(),
        Err(_) => {
            eprintln!("Error: Could not read directory {}", directory);
            return;
        }
    };

    if paths.is_empty() {
        eprintln!("Warning: No files found in directory {}", directory);
    }

    let start_time = Instant::now();
    let results: Vec<HashMap<String, (u64, f64)>> = paths.par_iter()
        .map(|path| process_file(path))
        .collect();
    let duration = start_time.elapsed();

    // Combine results
    let mut combined_result = HashMap::new();
    for result in results {
        for (project, (views, processed_views)) in result {
            let entry = combined_result.entry(project).or_insert((0, 0.0));
            entry.0 += views;
            entry.1 += processed_views;
        }
    }

    // Convert the HashMap to a Vec of tuples
    let mut result_vec: Vec<(String, (u64, f64))> = combined_result.into_iter().collect();

    // Write results to CSV
    let mut wtr = match WriterBuilder::new().from_path(output_file) {
        Ok(writer) => writer,
        Err(_) => {
            eprintln!("Error: Could not write to file {}", output_file);
            return;
        }
    };

    if let Err(_) = wtr.write_record(&["project", "views_sum", "processed_views_mean"]) {
        eprintln!("Error: Could not write header to file {}", output_file);
        return;
    }

    // Sort by the project column before writing to CSV
    result_vec.sort_by(|a, b| a.0.cmp(&b.0)); // Sort ascending by project name

    for (project, (views, processed_views)) in result_vec {
        if let Err(_) = wtr.write_record(&[
            &project,
            &views.to_string(),
            &(processed_views / views as f64).to_string(),
        ]) {
            eprintln!("Error: Could not write record for project {} to file {}", project, output_file);
        }
    }

    println!("Execution time: {:?}", duration);
}
