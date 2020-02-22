#!/usr/bin/perl
use mabain;
use strict;
use warnings;
use Data::Dumper;

sub insertion_test {
    my ($db_dir, $count) = @_;
    my $db = mabain::mb_open($db_dir, 1);
    my $added = 0;
    for(my $i = 0; $i < $count; $i++) {
        my $key = "TEST_KEY_$i";
        my $val = "TEST_VALUE_$i";
        my $rval = mabain::mb_add($db, $key, length($key), $val, length($val));
        if($rval == 0) {
            $added += 1;
        }
    }
    mabain::mb_close($db);
    print ("Successfully added $added KV pair\n"); 
}

sub query_test {
    my ($db_dir, $count) = @_;
    my $db = mabain::mb_open($db_dir, 1);
    my $found = 0;
    my $result = mabain::new_mb_query_result();
    for(my $i = 0; $i < $count; $i++) {
        my $key = "TEST_KEY_$i";
        my $rval = mabain::mb_find($db, $key, length($key), $result);
        if($rval == 0) {
            $found += 1;
            print(mabain::mb_query_result_data_get($result) . "\n");
        }
    }
    mabain::mb_close($db);
    print ("Successfully found $found KV pair\n"); 
}

my $db_dir = "/var/tmp/mabain_test";
my $count = 100;
system("mkdir -p /var/tmp/mabain_test");
system("rm -rf /var/tmp/mabain_test/*");


insertion_test($db_dir, $count);
query_test($db_dir, $count);
