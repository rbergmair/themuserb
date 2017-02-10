from datetime import datetime;
from urllib.parse import urlparse;
from struct import pack, unpack;



class JobsDBEntity:

  @classmethod
  def get_recs_from_datasource( cls, datasource ):
    
    assert False;

  @classmethod
  def dump_to_csv( cls, datasource, fn ):

    with open( fn, "wt", encoding="utf-8" ) as f:
      for rec in cls.get_recs_from_datasource( datasource ):
        cls.assert_is_valid( rec );
        f.write( cls.to_csv(rec) );



class Job( JobsDBEntity ):


  @classmethod
  def fn_csv( cls ):

    return "job.csv";


  @classmethod
  def create_table( cls ):

    return \
      [ """
          DROP TABLE IF EXISTS job;
        """,
        """
          CREATE TABLE job (
              job_id INTEGER,
              publication_date TIMESTAMP,
              short_name VARCHAR(256),
              name VARCHAR(1280),
              type_ CHAR(1),
              company_id INTEGER,
              landing_page VARCHAR(1280)
            );
        """ ];


  @classmethod
  def load_data( cls, basedir ):

    return \
      [ """
          DELETE FROM job
        """,
        """
          COPY job FROM '{:s}/{:s}' DELIMITER '\t';
        """\
         .format(
              basedir,
              cls.fn_csv()
            ) ];


  @classmethod
  def get_recs_from_datasource( cls, datasource ):
    
    for rec in datasource.get_jobs():
      yield rec;


  @classmethod
  def assert_is_valid( cls, rec ):

    ( job_id,
      publication_date,
      short_name,
      name,
      type_,
      company_id,
      landing_page ) = rec;

    assert isinstance( job_id, int );
    assert 0 <= job_id;
    assert unpack( "i", pack( "i", job_id ) )[ 0 ] == job_id;

    try:
      assert isinstance( publication_date, datetime );
    except:
      print( repr(publication_date) );
      raise;

    assert isinstance( short_name, str );
    assert "\t" not in short_name;
    assert len( short_name ) <= 256;

    assert isinstance( name, str );   
    assert "\t" not in name;
    assert len( name ) <= 1280;

    assert isinstance( type_, str );
    assert type_ in [ "X", "N" ];
    
    assert isinstance( company_id, int );
    assert 0 <= company_id;
    assert unpack( "i", pack( "i", company_id ) )[ 0 ] == company_id;

    assert isinstance( landing_page, str );
    urlparse( landing_page );
    assert "\t" not in landing_page;
    assert len( landing_page ) <= 1280;


  @classmethod
  def to_csv( cls, rec ):

    ( job_id,
      publication_date,
      short_name,
      name,
      type_,
      company_id,
      landing_page ) = rec;

    publication_date = publication_date.isoformat();

    rec \
      = ( job_id,
          publication_date,
          short_name,
          name,
          type_,
          company_id,
          landing_page );      

    return "\t".join( [ str(fld) for fld in rec ] ) + "\n";



class JobLocation( JobsDBEntity ):


  @classmethod
  def fn_csv( cls ):

    return "job_location.csv";


  @classmethod
  def create_table( cls ):

    return \
      [ """
          DROP TABLE IF EXISTS job_location;
        """,
        """
          CREATE TABLE job_location (
              job_id INTEGER,
              location_name VARCHAR(128)
            );
        """ ];


  @classmethod
  def load_data( cls, basedir ):

    return \
      [ """
          DELETE FROM job_location
        """,
        """
          COPY job_location FROM '{:s}/{:s}' DELIMITER '\t';
        """\
         .format(
              basedir,
              cls.fn_csv()
            ) ];


  @classmethod
  def get_recs_from_datasource( cls, datasource ):
    
    for rec in datasource.get_job_locations():
      yield rec;


  @classmethod
  def assert_is_valid( cls, rec ):

    ( job_id, location_name ) = rec;

    assert isinstance( job_id, int );
    assert 0 <= job_id;
    assert unpack( "i", pack( "i", job_id ) )[ 0 ] == job_id;

    assert isinstance( location_name, str );
    assert "\t" not in location_name;
    assert len(location_name) <= 128;



  @classmethod
  def to_csv( cls, rec ):

    return "\t".join( [ str(fld) for fld in rec ] ) + "\n";



class JobLevel( JobsDBEntity ):


  @classmethod
  def fn_csv( cls ):

    return "job_level.csv";


  @classmethod
  def create_table( cls ):

    return \
      [ """
          DROP TABLE IF EXISTS job_level;
        """,
        """
          CREATE TABLE job_level (
              job_id INTEGER,
              level_code CHAR(1)
            );
        """ ];


  @classmethod
  def load_data( cls, basedir ):

    return \
      [ """
          DELETE FROM job_level
        """,
        """
          COPY job_level FROM '{:s}/{:s}' DELIMITER '\t';
        """\
         .format(
              basedir,
              cls.fn_csv()
            ) ];


  @classmethod
  def get_recs_from_datasource( cls, datasource ):
    
    for rec in datasource.get_job_levels():
      yield rec;


  @classmethod
  def assert_is_valid( cls, rec ):

    ( job_id, level_code ) = rec;

    assert isinstance( job_id, int );
    assert 0 <= job_id;
    assert unpack( "i", pack( "i", job_id ) )[ 0 ] == job_id;

    assert isinstance( level_code, str );
    assert level_code in [ "E", "M", "S", "I" ];


  @classmethod
  def to_csv( cls, rec ):

    return "\t".join( [ str(fld) for fld in rec ] ) + "\n";



class JobCategory( JobsDBEntity ):


  @classmethod
  def fn_csv( cls ):

    return "job_category.csv";
    

  @classmethod
  def create_table( cls ):

    return \
      [ """
          DROP TABLE IF EXISTS job_category;
        """,
        """
          CREATE TABLE job_category (
              job_id INTEGER,
              category_name VARCHAR(128)
            );
        """ ];


  @classmethod
  def load_data( cls, basedir ):

    return \
      [ """
          DELETE FROM job_category
        """,
        """
          COPY job_category FROM '{:s}/{:s}' DELIMITER '\t';
        """\
         .format(
              basedir,
              cls.fn_csv()
            ) ];


  @classmethod
  def get_recs_from_datasource( cls, datasource ):
    
    for rec in datasource.get_job_categories():
      yield rec;


  @classmethod
  def assert_is_valid( cls, rec ):

    ( job_id, category_name ) = rec;

    assert isinstance( job_id, int );
    assert 0 <= job_id;
    assert unpack( "i", pack( "i", job_id ) )[ 0 ] == job_id;

    assert isinstance( category_name, str );
    assert "\t" not in category_name;
    assert len(category_name) <= 128;


  @classmethod
  def to_csv( cls, rec ):

    return "\t".join( [ str(fld) for fld in rec ] ) + "\n";



class JobTag( JobsDBEntity ):


  @classmethod
  def fn_csv( cls ):

    return "job_tag.csv";


  @classmethod
  def create_table( cls ):

    return \
      [ """
          DROP TABLE IF EXISTS job_tag;
        """,
        """
          CREATE TABLE job_tag (
              job_id INTEGER,
              tag_code CHAR(1)
            );
        """ ];


  @classmethod
  def load_data( cls, basedir ):

    return \
      [ """
          DELETE FROM job_tag
        """,
        """
          COPY job_tag FROM '{:s}/{:s}' DELIMITER '\t';
        """\
         .format(
              basedir,
              cls.fn_csv()
            ) ];


  @classmethod
  def get_recs_from_datasource( cls, datasource ):
    
    for rec in datasource.get_job_tags():
      yield rec;


  @classmethod
  def assert_is_valid( cls, rec ):

    ( job_id, tag_code ) = rec;

    assert isinstance( job_id, int );
    assert 0 <= job_id;
    assert unpack( "i", pack( "i", job_id ) )[ 0 ] == job_id;

    assert isinstance( tag_code, str );
    assert tag_code in [ "F", "G", "Y" ];


  @classmethod
  def to_csv( cls, rec ):

    return "\t".join( [ str(fld) for fld in rec ] ) + "\n";



class Company( JobsDBEntity ):


  @classmethod
  def fn_csv( cls ):

    return "company.csv";


  @classmethod
  def create_table( cls ):

    return \
      [ """
          DROP TABLE IF EXISTS company;
        """,
        """
          CREATE TABLE company (
              job_id INTEGER,
              short_name VARCHAR(128),
              name VARCHAR(1280)
            );
        """ ];


  @classmethod
  def load_data( cls, basedir ):

    return \
      [ """
          DELETE FROM company
        """,
        """
          COPY company FROM '{:s}/{:s}' DELIMITER '\t';
        """\
         .format(
              basedir,
              cls.fn_csv()
            ) ];


  @classmethod
  def get_recs_from_datasource( cls, datasource ):
    
    for rec in datasource.get_companies():
      yield rec;


  @classmethod
  def assert_is_valid( cls, rec ):

    ( company_id, sname, name ) = rec;

    assert isinstance( company_id, int );
    assert 0 <= company_id;
    assert unpack( "i", pack( "i", company_id ) )[ 0 ] == company_id;

    assert isinstance( sname, str );
    assert "\t" not in sname;
    assert len(sname) <= 128;

    assert isinstance( name, str );
    assert "\t" not in name;
    assert len(name) <= 1280;


  @classmethod
  def to_csv( cls, rec ):

    return "\t".join( [ str(fld) for fld in rec ] ) + "\n";



class JobsDBSchema:


  entities \
    = [ Job, JobLocation, JobLevel, JobCategory, JobTag, Company ];


  @classmethod
  def create_tables( cls ):

    for entity in cls.entities:
      for sqlstmt in entity.create_table():
        yield sqlstmt;


  @classmethod
  def load_data( cls, basedir ):

    for entity in cls.entities:
      for sqlstmt in entity.load_data( basedir ):
        yield sqlstmt;


  @classmethod
  def dump_to_csv( cls, datasource, basedir ):

    for entity in cls.entities:

      entity.dump_to_csv(
          datasource,
          basedir + "/" + entity.fn_csv()
        );
