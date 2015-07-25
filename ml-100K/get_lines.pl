#!/usr/bin/perl
my $filename=$ARGV[0];
my $destfilename=$ARGV[2];
open RH,"<$filename" or die "Cannot open $filename: $!";
my $nLines=$ARGV[1];

open WH, ">$destfilename" or die "Cannot write to $destfilename :$!";
while ($counter<=$nLines && <RH>)
{
	$counter++;
	$line=<RH>;
	print WH  $line;
 }
 close RH;
close WH

