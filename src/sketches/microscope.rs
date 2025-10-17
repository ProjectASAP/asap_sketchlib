use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MicroScope {
    pub window_size: usize,
    pub pixel_counters: Vec<u32>,
    pub zooming_counter: u32,
    pub shutter_counter: u32,
    pub c: u32,                 // probably the c of a*c^b in paper
    pub pixel_counter_size: u8, // number of bits used by each pixel counter
    pub sub_window_count: usize,
    pub n: usize, // current sub window
    pub log_base: i32,
}

impl MicroScope {
    pub fn debug(&self) -> () {
        println!("pixel counters: {:?}", self.pixel_counters);
        println!("zooming counter: {}", self.zooming_counter);
        println!("shutter counter: {}", self.shutter_counter);
    }

    pub fn init_microscope(w: usize, t: usize) -> Self {
        assert!(
            w % t == 0,
            "window size not a multiply of sub window count; integer sub window size impossible"
        );
        assert!(
            w > t,
            "too many sub windows; integer sub window size impossible"
        );
        MicroScope {
            window_size: w,
            pixel_counters: vec![0; t + 2],
            zooming_counter: 0,
            shutter_counter: 0,
            c: 2,
            pixel_counter_size: 4,
            sub_window_count: t + 2,
            n: 2 * (t + 2),
            log_base: 1, // use 1 for now
        }
    }

    fn zoom_out(&mut self) {
        self.zooming_counter += 1;
        // for i from 0 to (T+1)
        for i in 0..self.sub_window_count {
            self.pixel_counters[i] = self.pixel_counters[i] / self.c;
        }
    }

    fn zoom_in(&mut self) {
        if self.zooming_counter == 0 {
            return;
        }
        let mut flag = true;
        for i in 0..self.sub_window_count {
            if self.pixel_counters[i] >= (1 << self.pixel_counter_size) / self.c {
                flag = false;
            }
        }
        if flag {
            for i in 0..self.sub_window_count {
                self.pixel_counters[i] = self.pixel_counters[i] * self.c;
            }
            self.zooming_counter -= 1;
        }
    }

    fn carry_in(&mut self, pixel_idx: usize) {
        let threshold = (self.c).pow(self.zooming_counter);
        if self.shutter_counter >= threshold {
            self.shutter_counter = 0;
            self.pixel_counters[pixel_idx] += 1;
            // zoom-out operation
            if self.pixel_counters[pixel_idx] == (1 << self.pixel_counter_size) {
                self.zoom_out();
            }
        }
    }

    // not sure if the merge is correct or not, just have a merge as a place holder
    pub fn merge(&mut self, other: &MicroScope, ts: u64) {
        assert!(
            self.c == other.c,
            "c different, cannot merge these two MicroScope sketch"
        );
        assert!(
            self.log_base == other.log_base,
            "log_base different, cannot merge these two MicroScope sketch"
        );
        assert!(
            self.n == other.n,
            "n different, cannot merge these two MicroScope sketch"
        );
        assert!(
            self.window_size == other.window_size,
            "window size different, cannot merge these two MicroScope sketch"
        );
        assert!(
            self.sub_window_count == other.sub_window_count,
            "sub window count different, cannot merge these two MicroScope sketch"
        );
        assert!(
            self.pixel_counter_size == other.pixel_counter_size,
            "pixel counter size different, cannot merge these two MicroScope sketch"
        );
        self.zooming_counter += other.zooming_counter;
        self.shutter_counter += other.shutter_counter;
        for i in 0..self.sub_window_count {
            self.pixel_counters[i] += other.pixel_counters[i];
        }
        let sub_window_size = self.window_size as f64 / (self.sub_window_count as f64 - 2.0);
        let cur_sub_window = (ts as f64 / sub_window_size) as usize;
        let pixel_idx = cur_sub_window % self.sub_window_count;
        self.carry_in(pixel_idx);
    }

    pub fn insert(&mut self, timestamp: u64) {
        // first locate zero counter and current counter
        let sub_window_size = self.window_size as f64 / (self.sub_window_count as f64 - 2.0);
        let cur_sub_window = (timestamp as f64 / sub_window_size) as usize;
        let pixel_idx = cur_sub_window % self.sub_window_count;
        let zero_counter = self.pixel_counters[(cur_sub_window + 1) % self.sub_window_count];
        // second check zero counter and clear it
        if zero_counter != 0 {
            self.pixel_counters[(cur_sub_window + 1) % self.sub_window_count] = 0;
        }
        // third, increment shutter counter and carry-in
        self.shutter_counter += 1;
        self.carry_in(pixel_idx);
        // zoom-in operation
        self.zoom_in();
        return;
    }

    pub fn query(&self, timestamp: u64) -> f64 {
        let sub_window_size = self.window_size as f64 / (self.sub_window_count as f64 - 2.0);
        let cur_sub_window = (timestamp as f64 / sub_window_size) as usize;
        let zero_counter = (cur_sub_window + 1) % self.sub_window_count;
        // supposed to be: [(cur_sub_window-(sub_window_count-2)) % sub_window_count]
        let last_sub_window = (cur_sub_window + 2) % self.sub_window_count;
        let mut res = self.shutter_counter as f64;
        for i in 0..self.sub_window_count {
            if i != zero_counter && i != last_sub_window {
                res += self.pixel_counters[i] as f64 * (self.c.pow(self.zooming_counter) as f64);
            }
        }
        // linear approximation
        // the rate is... the porportion of subwindow that is still valid?
        let sub_window_size = (self.window_size / (self.sub_window_count - 2)) as u64;
        let rate = 1.0 - ((timestamp % sub_window_size) as f64 / sub_window_size as f64);
        let delta_f = rate * (self.pixel_counters[last_sub_window]) as f64;
        res += delta_f;
        return res;
    }

    pub fn delete(&mut self, timestamp: u64) {
        let sub_window_size = self.window_size as f64 / (self.sub_window_count as f64 - 2.0);
        let cur_sub_window = (timestamp as f64 / sub_window_size) as usize;
        let pixel_idx = cur_sub_window % self.sub_window_count;
        if self.shutter_counter > 0 {
            // case 1 shutter_count non-zero
            self.shutter_counter -= 1;
        } else if self.shutter_counter == 0 && self.pixel_counters[pixel_idx] > 0 {
            // case 2 shutter_count zero but P[cur] non-zero
            self.pixel_counters[pixel_idx] -= 1;
            let threshold = (self.c).pow(self.zooming_counter) - 1;
            self.shutter_counter = threshold;
        } else {
            // case 3 shutter_count and P[cur] both zero
            if pixel_idx == 0 {
                return;
            }
            let mut idx_to_dec = pixel_idx - 1;
            while idx_to_dec > 0 {
                if self.pixel_counters[idx_to_dec] > 0 {
                    self.pixel_counters[idx_to_dec] -= 1;
                    break;
                }
                idx_to_dec -= 1;
            }
            let threshold = (self.c).pow(self.zooming_counter) - 1;
            self.shutter_counter = threshold;
        }
    }
}
