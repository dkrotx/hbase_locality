#!/usr/bin/perl -w
use strict;
use Socket;

my %region2srv;
my %regionserver_stat;

my $cur_region;
my $listblocks = 0;
my ( $total_length, $hit_length ) = ( 0, 0 );

# Hadoop gives only IP
my %cached_addrs;
sub Ip2Host
{
	my $ip = shift;
	
	unless ( exists $cached_addrs{$ip} ) { 
		$cached_addrs{$ip} = gethostbyaddr( inet_aton($ip), AF_INET ) or die "failed to get host by \"$ip\"";
	}
	return $cached_addrs{$ip};
}

sub AccountBlock
{
	my ( $len, $servers_str ) = @_;
	my $host_region = $region2srv{$cur_region}; # machine which serve region
	my @servers = split(/,\s+/, $servers_str);
	my $found = 0;	
	
	$total_length += $len;
	foreach (@servers)
	{
		my ( $ipaddr ) = ( $_ =~ /^(.*):\d+$/ );
		my $host = Ip2Host($ipaddr);
		if ( $host_region eq $host ) {
			$found = 1; # hit to RS host
			last;
		}
	}

	$regionserver_stat{$host_region}->{total_size} += $len;
	if ( $found ) {
		$regionserver_stat{$host_region}->{hit_size} += $len;
	}
	else {
		$regionserver_stat{$host_region}->{miss_size} += $len;
	}
}

sub ShortSize { return $_[0] / 2**30 }


##
## MAIN
##

my $tablename = quotemeta($ARGV[0]);
shift @ARGV;

open(my $F, '<', $ARGV[0]) or die "failed to open distribution map";
while(<$F>)
{
	if (/(\w+)\s(.*)/) {
		$region2srv{$1} = $2;
	}
}
close($F);
print scalar(keys %region2srv) . " regions loaded\n";
shift @ARGV;


my($nblocks, $nfiles) = 0;

# read block-distribusion statistics
while(<>)
{
	if ( m%/hbase/$tablename/(\w+)/.* \d+ bytes, \d+ block\(s\):% ) {
		$cur_region = $1;
		$listblocks = 1;
		$nfiles++;
	}
	elsif ($listblocks)
	{
		if (/^\d+\. blk_\S+ len=(\d+) repl=\d+ \[(.*)\]$/) {
			AccountBlock($1, $2);
			$nblocks++;
		}
		else  {
			$listblocks = 0;
		}
	}
}

print "Have $nfiles files ($nblocks blocks total)\n\n";

printf( "%-15s%15s%15s%15s%15s\n", "REGIONSERVER", "TOTAL(Gb)", "HITSIZE", "MISSSIZE", "HITRATE" );
while ( my ($h, $st) = each(%regionserver_stat) )
{
	printf("%-15s%15d%15d%15d%15d%%\n", $h,
		ShortSize($regionserver_stat{$h}->{total_size} || 0),
		ShortSize($regionserver_stat{$h}->{hit_size} || 0),
		ShortSize($regionserver_stat{$h}->{miss_size} || 0),
		($regionserver_stat{$h}->{hit_size} / $regionserver_stat{$h}->{total_size}) * 100,
	);
}
