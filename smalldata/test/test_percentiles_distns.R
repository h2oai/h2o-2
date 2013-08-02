# generate modal dataset for testing histogram

n <- 1000

df <- data.frame( zeroone=sample( c(0,1), size=n, replace=T) )
df$zerotwo=sample( c(0:2), size=n, replace=T)



df$onemode_low0 <- rgamma( n=n, shape=1, scale=2 )

df$onemode_low1 <- rgamma( n=n, shape=2, scale=2 )

df$onemode_hi <- rgamma( n=n, shape=200, scale=0.1 )

df$twomode <- df$zeroone * rgamma( n=n, shape=1, scale=2) + (1 - df$zeroone) *  rgamma( n=n, shape=20, scale=0.5 )

df$threemode <- ifelse( df$zerotwo == 0, rgamma(n=n, shape=1, scale=2), ifelse( df$zerotwo == 1, rgamma( n=n, shape=20, scale=0.5 ), rgamma(n=n, shape=40, scale=0.5)))

# add some factors
df$zerooneF <- paste('f', df$zeroone )
df$zerotwoF <- paste('f', df$zerotwo )

file <- gzfile( 'test_percentiles_distns.csv.gz', 'w' )
write.csv(file=file, x=df, row.names=F)
close( file )
