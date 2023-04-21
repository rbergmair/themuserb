from themuserb.themuse import TheMuse;
from themuserb.jobsdb import JobsDBSchema;
from themuserb import cfg;



def main( basedir=None, pages_limit=None ):

  if pages_limit is not None:
    assert isinstance( pages_limit, str );
    pages_limit = int( pages_limit );

  themuse = TheMuse();
  themuse.download_jobs( pages_limit );
  JobsDBSchema.dump_to_csv( themuse, basedir or cfg.BASEDIR_CLIENT_DFLT );



if __name__ == "__main__":

  import sys;
  main( *sys.argv );
