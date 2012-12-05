#!/usr/bin/perl -w
use strict;

my $capture = 0;
my $server;
my $record;

sub printRecord
{
	my ( $region_id ) = ( $record =~ /\.(\w+)\.$/ ) or die "bad region record";
	print "$region_id\t$server\n";
}

my $tablename = quotemeta($ARGV[0]);
shift @ARGV;

while(<>)
{
	if (/^\s$tablename,(.*)\scolumn=info:server.*value=(.*):\d+/)
	{
		printRecord() if ($capture);
		$record = $1;
		$server = $2;
		$capture = 1;
	}
	elsif (/column=/ && $capture) {
		printRecord();
		$capture = 0;
	}
	elsif ($capture) {
		$_ =~ s/\s//g;
		$record .= $_;
	}
}
