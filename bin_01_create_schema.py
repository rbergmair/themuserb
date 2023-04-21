import psycopg2;


from themuserb import cfg;
from themuserb.jobsdb import JobsDBSchema;



def main( host, user, password, dbname ):

  connstr \
    =   "host={} ".format( host ) \
      + "user={} ".format( user ) \
      + "password={} ".format( password ) \
      + "dbname={} ".format( dbname );

  conn = psycopg2.connect( connstr );
  try:

    for stmt in JobsDBSchema.create_tables():

      print( stmt );

      cursor = conn.cursor();
      try:
        cursor.execute( stmt );
      finally:
        cursor.close();

  finally:
    conn.commit();
    conn.close();



if __name__ == "__main__":

  import sys;
  main( *sys.argv );
