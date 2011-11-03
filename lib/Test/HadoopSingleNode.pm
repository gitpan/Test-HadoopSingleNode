package Test::HadoopSingleNode;

use strict;
use warnings;

use 5.008;
use Carp;

use File::Temp qw(tempdir);
use Test::TCP qw(wait_port empty_port);
use File::Which qw(which);
use Proc::Guard qw(proc_guard);

use IPC::Open3 qw(open3);
use POSIX ":sys_wait_h";
use IO::Select;
use IO::Handle;
use Symbol qw(gensym);
use IPC::Run qw(harness timeout);

use XML::Simple;
use Data::Rmap qw(rmap);
use Path::Class qw(dir file);
use File::Copy qw(copy);
use Data::Dumper;

our $VERSION = '0.02_03';

# I tested at hadoop-0.20.2-cdh3u1
our $conf_files = +{
    'core-site.xml' => +{
        property => [
            +{
                name => "hadoop.tmp.dir",
                value => "__TMP_DIR__/tmp",
            },
            +{
                name => "fs.default.name",
                value => "hdfs://localhost:__FS_PORT__",
            },
        ],
    },
    'mapred-site.xml' => +{
        property => [
            +{
                name => "mapred.job.tramrer",
                value => "hdfs://localhost:__MR_PORT__",
            },
        ],
    },
    'hdfs-site.xml' => +{
        property => [
            +{
                name => "dfs.replication",
                value => 1,
            },
        ],
    },
};

our @HADOOP_DAEMON_PROCESS_NAMES = qw/
org.apache.hadoop.hdfs.server.namenode.NameNode
org.apache.hadoop.hdfs.server.datanode.DataNode
/;

my $DEBUG_MSG = 1;

use Class::Accessor::Lite (
    rw => [ qw(hadoop_conf_dir fs_port mr_port) ],
);

#-------------------------------------------------------------------------------
sub new {
    my ($class, %args) = @_;
    my $self = bless +{
        fs_port => empty_port,
        mr_port => empty_port,
        running_hadoop_conf_dir => $args{running_hadoop_conf_dir} || $ENV{TEST_HADOOP_SINGLENODE_CONF},
        default_hadoop_conf_dir => $args{default_hadoop_conf_dir},
        _RUNNING_TEST_HADOOP => 0,
        _fixture_files => [],
    }, $class;
    $self->{hadoop_conf_dir} = $self->_set_hadoop_conf_dir() || die "conf dir specified";
    $self;
}

sub DESTROY {
    my $self = shift;
    $self->stop_all;
}

sub start_all {
    my ($self) = @_;
    # Use running hadoop
    if ($self->{running_hadoop_conf_dir}) {
        die "hadoop isnt running" unless (__check_alived_hadoop());
        warn "hadoop is already running" if ($DEBUG_MSG);
        $self->{_RUNNING_TEST_HADOOP} = 1;
        return;
    }
    # Check running hadoop process
    if (my @pids = __check_alived_hadoop()) {
        die 'hadoop is running. (' . join(',', @pids) . ')';
    }
    # Format namenode
    $self->_cmd_harness_y_only( scalar(which('hadoop')), $self->_add_hadoop_opt, 'namenode', '-format' );
    # Start hdfs
    my $dfs_proc = proc_guard( scalar(which('start-dfs.sh')), $self->_add_hadoop_opt );
    # Start mapred
    my $mr_proc = proc_guard( scalar(which('start-mapred.sh')), $self->_add_hadoop_opt );
    while ( __check_alived_hadoop(@HADOOP_DAEMON_PROCESS_NAMES) < scalar @HADOOP_DAEMON_PROCESS_NAMES ) {
        sleep 3;
        print '.';
    }
    $self->{_RUNNING_TEST_HADOOP} = 1;
}

sub construct_fixture {
    my ($self, %args) = @_;
    for my $dist (keys %args) {
        my $local = $args{$dist};
        die "$local not found" unless (-e $local);
        my ($ret, $out, $err)
            = $self->cmd( scalar(which('hadoop')), $self->_add_hadoop_opt, 'fs', '-put', $local, $dist );
        carp "hdfs put : $local => $dist" if ($DEBUG_MSG);
        carp "failed to put on hdfs($ret) : $err" if ($ret != 0);
        push @{$self->{_fixture_files}}, $dist;
    }
}

sub stop_all {
    my ($self) = @_;
    return if ($self->{running_hadoop_conf_dir});
    return if (scalar __check_alived_hadoop() == 0);
    return if ($self->{_RUNNING_TEST_HADOOP} == 0);
    $self->cmd(scalar(which('stop-all.sh')), $self->_add_hadoop_opt );
    if (__check_alived_hadoop()) {
        carp join ' ', "Perhaps fail to stop hadoop daemon."
            , "Please stop daemons manually."
            , "ex: \$HADOOP_HOME/bin/stop-all.sh --config \$HADOOP_CONF_DIR"
            , "or ", scalar(which('stop-all.sh')), $self->_add_hadoop_opt;
    }
    else {
        $self->{_RUNNING_TEST_HADOOP} = 0;
    }
}

sub cleanup {
    my ($self) = @_;
    for (@{$self->{_fixture_files}}) {
        $self->cmd( scalar(which('hadoop')), $self->_add_hadoop_opt, 'fs', '-rmr', $_ );
    }
}

#-------------------------------------------------------------------------------

sub _set_hadoop_conf_dir {
    my ($self) = @_;

    return $self->{running_hadoop_conf_dir} if ($self->{running_hadoop_conf_dir});

    my $hadoop_conf_dir = tempdir();
    my $log_dir  = "$hadoop_conf_dir/log";
    mkdir($log_dir, 0700) unless (-d $log_dir);
    # Replace confs
    __replace_data($conf_files,
        +{
            __TMP_DIR__ => $hadoop_conf_dir,
            __FS_PORT__ => $self->fs_port,
            __MR_PORT__ => $self->mr_port,
            __LOG_DIR__ => $log_dir,
        }
    );
    # Copy default configs
    if (defined $self->{default_hadoop_conf_dir}
        && -d $self->{default_hadoop_conf_dir}) {
        my $dir = dir( $self->{default_hadoop_conf_dir});
        while (my $file = $dir->next ) {
            copy($file, $hadoop_conf_dir);
        }
    }
    # Overwrite configs
    for my $filename (keys %{$conf_files}) {
        if ($filename =~ /.xml$/) {
            XML::Simple->new()->XMLout(
                $conf_files->{$filename},
                XMLDecl => '<?xml version="1.0"?>',
                NoAttr => 1,
                RootName => "configuration",
                outputfile => "$hadoop_conf_dir/$filename",
            );
        }
        else {
            open my $fh, '>', "$hadoop_conf_dir/$filename" or die $!;
            for my $content (@{$conf_files->{$filename}}) {
                print $fh $content, "\n";
            }
            close $fh;
        }
    }
    $hadoop_conf_dir;
}

sub cmd {
    my ($self, @cmd) = @_;
    carp join(' ', 'exec command:', @cmd) if ($DEBUG_MSG);
    my ($output, $error) = ('', '');
    my ($rdr, $err) = (gensym, gensym);
    my $pid = IPC::Open3::open3(undef, $rdr, $err, @cmd);
    my $reader = new IO::Select($rdr, $err);

    while (1) {
        while (my @ready = $reader->can_read()) {
            foreach my $fh (@ready) {
                my $data;
                my $length = sysread $fh, $data, 4096;
                if( ! defined $length || $length == 0 ) {
                    $err .= "Error from child: $!\n" unless defined $length;
                    $reader->remove($fh);
                } else {
                    if ($fh == $rdr) {
                        $output .= $data;
                    } elsif ($fh == $err) {
                        $error .= $data;
                    } else {
                        die "BUG: got an unexpected filehandle:" . Dumper($data);
                    }
                }
            }
        }
        if (waitpid( $pid, WNOHANG ) > 0) {
            last;
        }
    }
    my $retcode = WIFEXITED($?) ? WEXITSTATUS($?) : WTERMSIG($?);
    return wantarray ? ($retcode, $output, $error) : $retcode;
}

sub _cmd_harness_y_only {
    my ($self, @cmd) = @_;
    carp join(' ', @cmd) if ($DEBUG_MSG);
    local $ENV{LANG} = "C";
    eval {
        my ($in, $out, $err);
        my $h = IPC::Run::harness(\@cmd, \$in, \$out, \$err, timeout( 600 ));
        $h->start;
        $in .= "Y\n";
        $h->finish;
        print $out if ($DEBUG_MSG);
        warn  $err if ($DEBUG_MSG);
    };
    if ($@) {
        die join(' ', @cmd, 'error', $@);
    }
}

sub _add_hadoop_opt {
    my ($self, @args) = @_;
    my @params;
    push @params, ('--config', $self->{hadoop_conf_dir})
        if (exists $self->{hadoop_conf_dir} && $self->{hadoop_conf_dir});
    @params;
}

#-------------------------------------------------------------------------------
# Utility methods

sub __check_alived_hadoop {
    my @process_names = @_;
    if (my $ps = which('ps')) {
        my $cmd =
              ($^O =~ /linux/i)  ? qq|$ps -n -o "%p %a"|     # linux
            : ($^O =~ /darwin/i) ? qq|$ps -Ao "pid command"| # osx
            : '';
        return unless $cmd;
        my @pids;
        my $regex = (@process_names)
            ? '(' . join('|', map { $_ =~ s/\./\\./go; $_; } @process_names) . ')'
            : '';
        for my $ps_cmd (split "\n", `$cmd`) {
            next if ($regex && $ps_cmd !~ /$regex/i);
            if ($ps_cmd =~ /\s*(\d+).*org\.apache\.hadoop/i) {
                push @pids, $1;
            }
        }
        return @pids;
    }
}

sub __replace_data {
    my ($stash, $replace) = @_;
    for my $key (keys %{$replace}) {
        rmap { s/$key/$replace->{$key}/ unless ref $_ } $stash;
    }
}

sub __dd {
    my (@args) = @_;
    return unless $DEBUG_MSG;
    warn map { Dumper $_ } @args;
}

1;
__END__

=head1 NAME

Test::HadoopSingleNode - Single node hadoop module for tests.

=head1 SYNOPSIS

  use Test::More;
  use Test::HadoopSingleNode;

  # Load from $HADOOP_DIR/conf
  my $hadoop = Test::HadoopSingleNode->new(
    default_hadoop_conf_dir => "./hadoop_conf",
  );

  $hadoop->start_all();

  $hadoop->construct_fixture(
    '/path/to/hdfs/file' => '/path/to/local/file',
  );

  $hadoop->stop_all();

=head1 DESCRIPTION

Test::HadoopSingleNode is automatically setups a hadoop instance in a temporary directory, and destroys it when the perl script exits.

=head1 FUNCTIONS

=head2 new

Set hadoop configuration. Default configuration is load filed from default_hadoop_conf_dir or $Test::HadoopSingleNode::conf_files, and set make a temporary dir.

=head2 start_all

Launch a hadoop instance.

=head2 construct_fixture

Put a fixture file to a hdfs.

=head2 stop_all

Stop the launched instance.

=head1 AUTHOR

Test::HadoopSingleNode E<lt>ya.yohei@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
