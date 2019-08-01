use crate::constants;
use crate::constants::{SF_DATE, SF_EMAIL, SF_IP, SF_PHONE, SF_QUOTED, SF_URL, SF_USER_AGENT};
use crate::query::QueryParsing;
use hyperscan::*;
use log::debug;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

pub const P_TEST: usize = 0;
pub const P_EMAIL: usize = 1;
pub const P_IP: usize = 2;
pub const P_QUOTED: usize = 3;
pub const P_DATE: usize = 4;
pub const P_PHONE: usize = 5;
pub const P_USER_AGENT: usize = 6;
pub const P_URL: usize = 7;

pub fn build_hs_db(flags: &constants::ScanFlags) -> BlockDatabase {
    let pattern_list: HashMap<usize, String> = [
        (P_TEST, "test".to_string()),
        (P_EMAIL, "([\\w\\.!#$%&'*+\\-=?\\^_`{|}~]+@([\\w\\d-]+\\.)+[\\w]{2,4})".to_string()),
        (P_IP, "(((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9])\\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9]))".to_string()),
        (P_QUOTED, "((\"(.*?)\")|'(.*?)')".to_string()),
        (P_DATE, "((19[789]\\d|2\\d{3})[-/](0[1-9]|1[1-2])[-/](0[1-9]|[1-2][0-9]|3[0-1]*))|((0[1-9]|[1-2][0-9]|3[0-1]*)[-/](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|(0[1-9]|1[1-2]))[-/](19[789]\\d|2\\d{3}))".to_string()),
        (P_PHONE, "[\\(]?(\\d{3})[\\)-]?[- ]?(\\d{3})[- ]?(\\d{4})".to_string()),
        (P_USER_AGENT, "\"((Mozilla|Links).*? \\(.*?\\)( .*?[0-9]{1,3}\\.[0-9]{1,3}\\.?[0-9]{0,3})?)\"".to_string()),
        (P_URL, "(https?|ftp)://[^\\s/$.?#].[^()\\]\\[\\s]*".to_string()),
    ].iter().cloned().collect();

    let mut patterns: Vec<Pattern> = Vec::new();

    if flags.contains(constants::ScanFlags::IP) {
        patterns.push(Pattern {
            expression: pattern_list.get(&P_IP).unwrap().clone(),
            id: P_IP.clone(),
            flags: CompileFlags(HS_FLAG_CASELESS | HS_FLAG_SOM_LEFTMOST),
        });
    }
    if flags.contains(constants::ScanFlags::EMAIL) {
        patterns.push(Pattern {
            expression: pattern_list.get(&P_EMAIL).unwrap().clone(),
            id: P_EMAIL.clone(),
            flags: CompileFlags(HS_FLAG_CASELESS | HS_FLAG_SOM_LEFTMOST),
        });
    }
    if flags.contains(constants::ScanFlags::DATE) {
        patterns.push(Pattern {
            expression: pattern_list.get(&P_DATE).unwrap().clone(),
            id: P_DATE.clone(),
            flags: CompileFlags(HS_FLAG_CASELESS | HS_FLAG_SOM_LEFTMOST),
        });
    }
    if flags.contains(constants::ScanFlags::QUOTED) {
        patterns.push(Pattern {
            expression: pattern_list.get(&P_QUOTED).unwrap().clone(),
            id: P_QUOTED.clone(),
            flags: CompileFlags(HS_FLAG_CASELESS | HS_FLAG_SOM_LEFTMOST),
        });
    }
    if flags.contains(constants::ScanFlags::URL) {
        patterns.push(Pattern {
            expression: pattern_list.get(&P_URL).unwrap().clone(),
            id: P_URL.clone(),
            flags: CompileFlags(HS_FLAG_CASELESS | HS_FLAG_SOM_LEFTMOST),
        });
    }
    if flags.contains(constants::ScanFlags::PHONE) {
        patterns.push(Pattern {
            expression: pattern_list.get(&P_PHONE).unwrap().clone(),
            id: P_PHONE.clone(),
            flags: CompileFlags(HS_FLAG_CASELESS | HS_FLAG_SOM_LEFTMOST),
        });
    }
    if flags.contains(constants::ScanFlags::USER_AGENT) {
        patterns.push(Pattern {
            expression: pattern_list.get(&P_USER_AGENT).unwrap().clone(),
            id: P_USER_AGENT.clone(),
            flags: CompileFlags(HS_FLAG_CASELESS | HS_FLAG_SOM_LEFTMOST),
        });
    }

    let db: BlockDatabase = patterns.build().unwrap();
    db
}

pub struct HSPatternMatch {
    pub pattern_id: u32,
    pub from: u64,
    pub to: u64,
}

pub struct HSLineScanner<'a> {
    //    pub inner: Arc<Mutex<LineScannerData>>,
    pub lines: &'a Vec<String>,
    pub line_matches: HashMap<u16, HashMap<u16, Vec<HSPatternMatch>>>,
}

pub type HSPatternMatchResults =
    Arc<RwLock<HashMap<u16, RwLock<HashMap<u16, Vec<HSPatternMatch>>>>>>;

impl<'a> HSLineScanner<'a> {
    pub fn new(lines: &Vec<String>) -> HSLineScanner {
        HSLineScanner {
            lines: lines,
            line_matches: HashMap::new(),
        }
    }

    pub fn scan(&mut self, db: &mut BlockDatabase) -> HSPatternMatchResults {
        let now = Instant::now();

        let line_total = self.lines.len();
        let scratch = db.alloc().unwrap();

        let pattern_match_results: HSPatternMatchResults = Arc::new(RwLock::new(HashMap::new()));

        for i in 0..line_total {
            db.scan_mut(
                &self.lines[i][..],
                0,
                &scratch,
                Some(callback_block),
                Some(&mut HSScanPair {
                    line: &self.lines[i],
                    line_index: i as u16,
                    pattern_match_results: Arc::clone(&pattern_match_results),
                }),
            )
            .unwrap();
        }

        debug!("scan completed in {:?}", now.elapsed());

        pattern_match_results
    }
}

struct HSScanPair<'a> {
    pub line: &'a String,
    pub pattern_match_results: HSPatternMatchResults,
    pub line_index: u16,
}

fn callback_block(id: u32, from: u64, to: u64, _flags: u32, context: &mut HSScanPair) -> u32 {
    //  Get the patterns matched for this line, else insert new map
    let mut line_map = context.pattern_match_results.write().unwrap();

    if line_map.contains_key(&context.line_index) == false {
        line_map.insert(context.line_index.clone(), RwLock::new(HashMap::new()));
    }

    let mut line_patterns = line_map
        .get_mut(&context.line_index)
        .unwrap()
        .write()
        .unwrap();

    if line_patterns.contains_key(&(id as u16)) == false {
        line_patterns.insert(id.clone() as u16, Vec::new());
    }

    let pattern_matches = line_patterns.get_mut(&(id as u16)).unwrap();

    // Get the matches for this pattern within the line

    // if this is the first match, insert
    if pattern_matches.len() == 0 {
        pattern_matches.push(HSPatternMatch {
            pattern_id: id,
            from: from,
            to: to,
        });
    } else {
        // else compare to previous matches to make sure we only keep the longest
        let mut collision = false;
        for i in 0..pattern_matches.len() {
            // if we have another pattern starting in the same spot, we probably have an overlap
            // keep the longest
            if pattern_matches[i].from == from && pattern_matches[i].to < to {
                collision = true;
                pattern_matches[i] = HSPatternMatch {
                    pattern_id: id,
                    from: from,
                    to: to,
                };
            }
        }
        if collision == false {
            pattern_matches.push(HSPatternMatch {
                pattern_id: id,
                from: from,
                to: to,
            });
        }
    }

    0
}

pub fn alloc_result_map(flags: &constants::ScanFlags) -> HashMap<String, Vec<String>> {
    let mut results: HashMap<String, Vec<String>> = HashMap::new();

    if flags.contains(constants::ScanFlags::IP) {
        results.insert(SF_IP.to_string(), Vec::new());
    }
    if flags.contains(constants::ScanFlags::EMAIL) {
        results.insert(SF_EMAIL.to_string(), Vec::new());
    }
    if flags.contains(constants::ScanFlags::DATE) {
        results.insert(SF_DATE.to_string(), Vec::new());
    }
    if flags.contains(constants::ScanFlags::QUOTED) {
        results.insert(SF_QUOTED.to_string(), Vec::new());
    }
    if flags.contains(constants::ScanFlags::URL) {
        results.insert(SF_URL.to_string(), Vec::new());
    }
    if flags.contains(constants::ScanFlags::PHONE) {
        results.insert(SF_PHONE.to_string(), Vec::new());
    }
    if flags.contains(constants::ScanFlags::USER_AGENT) {
        results.insert(SF_USER_AGENT.to_string(), Vec::new());
    }
    results
}

pub fn found_patterns_in_line(
    pattern_match_results: HSPatternMatchResults,
    line_index: &u16,
    query_data: &QueryParsing,
    line: &String,
) -> HashMap<String, Vec<String>> {
    // Retain only the lines with matches
    let read_match_hold = pattern_match_results.read().unwrap();
    let mut found_vals: HashMap<String, Vec<String>> = alloc_result_map(&query_data.scan_flags);
    // only the lines reported in pattern_match_results have the desired projections
    if read_match_hold.contains_key(line_index) {
        let patterns = read_match_hold.get(line_index).unwrap();

        let patterns_data = patterns.read().unwrap();

        for (pat_id, datum) in &*patterns_data {
            for pm in datum {
                match *pat_id as usize {
                    P_IP => {
                        println!("found IP!");
                        found_vals
                            .get_mut(SF_IP)
                            .unwrap()
                            .push(line[pm.from as usize..pm.to as usize].to_string());
                    }
                    P_EMAIL => {
                        found_vals
                            .get_mut(SF_EMAIL)
                            .unwrap()
                            .push(line[pm.from as usize..pm.to as usize].to_string());
                    }
                    P_DATE => {
                        found_vals
                            .get_mut(SF_DATE)
                            .unwrap()
                            .push(line[pm.from as usize..pm.to as usize].to_string());
                    }
                    P_QUOTED => {
                        found_vals
                            .get_mut(SF_QUOTED)
                            .unwrap()
                            .push(line[(pm.from + 1) as usize..(pm.to - 1) as usize].to_string());
                    }
                    P_URL => {
                        found_vals
                            .get_mut(SF_URL)
                            .unwrap()
                            .push(line[pm.from as usize..pm.to as usize].to_string());
                    }
                    P_PHONE => {
                        found_vals
                            .get_mut(SF_PHONE)
                            .unwrap()
                            .push(line[pm.from as usize..pm.to as usize].to_string());
                    }
                    P_USER_AGENT => {
                        found_vals
                            .get_mut(SF_USER_AGENT)
                            .unwrap()
                            .push(line[pm.from as usize..pm.to as usize].to_string());
                    }
                    _ => (),
                }
            }
        }
    }
    found_vals
}
