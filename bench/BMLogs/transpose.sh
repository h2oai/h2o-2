awk -F, '
{
  for(c = 1; c <= NF; c++) {
    a[c, NR] = $c
  }
  if(max_nf < NF) {
    max_nf = NF
  }
}
END {
  for(r = 1; r <= max_nf; r++) {
    for(c = 1; c <= NR; c++) {
        if (c == NR) {
            printf("%s",a[r,c])
        } else {
            printf("%s,", a[r, c])
        }
    }
    print ""
  }
}
' $1
