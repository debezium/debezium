CREATE DIRECTORY mydir AS '/scratch/data/file_data';
CREATE OR REPLACE DIRECTORY bfile_dir AS '/usr/bin/bfile_dir';
CREATE OR REPLACE DIRECTORY bfile_dir SHARING = NONE AS '/usr/bin/bfile_dir';
