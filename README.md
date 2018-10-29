# CopernicusProducts
Program which downloads products from Copernicus, checks md5, and makes a log to database about product

In order to run this program:

1. Establish mysql connection.
  Databse name should be ESA_DB
  grant all privileges on *.* to 'esa_user'@'localhost' identified by 'esa_password';
 
2. Create table in databse.
    create table esa_content
  (
    id int not null auto_increment primary key,
    filename varchar(255) not null,
    path varchar(255) not null,
    md5_checksum varchar(32) not null
);

3.Example of program initiation: python my_task.py 2018-06-02T00:00:00.00Z 2018-06-03T00:00:00.00Z 
