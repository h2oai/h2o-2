// OpenCL kernels

kernel void Softmax_fprop(int iLen, global const float* ia, global const float* w, global const float* b, global float* a) {
  int o = get_global_id(0);
  a[o] = 0;
  for( int i = 0; i < iLen; i++ )
    a[o] += w[o * iLen + i] * ia[i];
  a[o] += b[o];
}

kernel void Softmax_bprop(int iLen, global const float* ia, global float* w, global float* b, global const float* a, global const float* e, float rate, global float* ie) {
  int o = get_global_id(0);
  float g = e[o] * (1 - a[o]) * a[o];
  float u = rate * g;
  for( int i = 0; i < iLen; i++ ) {
    int w = o * iLen + i;
    ie[i] += g * w[w];
    w[w] += u * ia[i];
  }
  b[o] += u;
}

//

kernel void Tanh_fprop(int iLen, global const float* ia, global const float* w, global const float* b, global float* a) {
  int o = get_global_id(0);
  a[o] = 0;
  for( int i = 0; i < iLen; i++ )
    a[o] += w[o * iLen + i] * ia[i];
  a[o] += b[o];
  a[o] = tanh(a[o]);
}

kernel void Tanh_bprop(int iLen, global const float* ia, global float* w, global float* b, global const float* a, global const float* e, float rate) {
  int o = get_global_id(0);
  float g = e[o] * (1 - a[o] * a[o]);
  float u = rate * g;
  for( int i = 0; i < iLen; i++ ) {
    int w = o * iLen + i;
    w[w] += u * ia[i];
  }
  b[o] += u;
}

//

kernel void reset_error(const float* e) {
  int o = get_global_id(0);
  e[o] = 0;
}