/// A Buffer represents a segment of sending or receiving data (maybe unfinished).
#[derive(Debug, Default)]
pub struct Buffer {
    inner: Vec<u8>,
    cur_pos: usize,
}

impl Buffer {
    pub fn from_vec(v: Vec<u8>) -> Self {
        Buffer {
            inner: v,
            cur_pos: 0,
        }
    }

    pub fn with_len(len: usize) -> Self {
        let mut inner = Vec::with_capacity(len);
        unsafe { inner.set_len(len); }
        Buffer {
            inner,
            cur_pos: 0,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        assert!(self.is_clear());
        &self.inner
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        assert!(self.is_clear());
        &mut self.inner
    }

    pub fn mark_handled(&mut self, nbytes: usize) {
        self.cur_pos += nbytes;
        assert!(self.cur_pos <= self.inner.len());
    }

    pub fn is_clear(&self) -> bool {
        self.cur_pos == self.inner.len()
    }

    pub fn get_remain_buffer(&self) -> &[u8] {
        &self.inner[self.cur_pos..]
    }

    pub fn get_remain_buffer_mut(&mut self) -> &mut [u8] {
        &mut self.inner[self.cur_pos..]
    }
}
