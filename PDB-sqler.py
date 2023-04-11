from Bio.PDB import PDBParser
import psycopg2
import os
from tqdm import tqdm
import warnings
from Bio import BiopythonWarning

warnings.simplefilter('ignore', BiopythonWarning)

# Define the name of the PDB file and the database credentials
pdb_file = "A:\PDB\pdb1a9g.ent"
database_name = 'dvdrental'
database_user = 'postgres'
database_password = 'pass'
database_host = 'localhost'
database_port = '5432'

# Connect to the database
conn = psycopg2.connect(database=database_name, user=database_user, password=database_password,
                        host=database_host, port=database_port)
cur = conn.cursor()

# Create the table to store the PDB data

# Check if table exists and create it if it doesn't
cur.execute("""
    SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_name = 'pdb_files'
    )
""")
if not cur.fetchone()[0]:
    cur.execute("""
        CREATE TABLE pdb_files (
            file_name TEXT,
            atom_name TEXT,
            atom_id TEXT,
            residue_name TEXT,
            chain_id TEXT,
            residue_id TEXT,
            x TEXT,
            y TEXT,
            z TEXT
        )
    """)
    conn.commit()

# Iterate through PDB files in folder
pdb_folder = "A:\Sample"
pdb_files = [f for f in os.listdir(pdb_folder) if f.endswith(".ent")]

for file_name in tqdm(pdb_files, desc="Processing files"):
    # Parse PDB file
    parser = PDBParser()
    structure = parser.get_structure(file_name, os.path.join(pdb_folder, file_name))

    # Extract data from structure
    for model in structure:
        for chain in model:
            for residue in chain:
                for atom in residue:
                    atom_name = atom.get_name()
                    atom_id = atom.get_serial_number()
                    residue_name = residue.get_resname()
                    chain_id = chain.get_id()
                    residue_id = residue.get_id()[1]
                    x, y, z = map(float, atom.get_coord())

                    # Insert data into PostgreSQL database
                    cur.execute(
                        "INSERT INTO pdb_files (file_name, atom_name, atom_id, residue_name, chain_id, residue_id, x, y, z) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (file_name, atom_name, atom_id, residue_name, chain_id, residue_id, x, y, z)
                    )
                    conn.commit()

# Close database connection
file_list = os.listdir(pdb_folder)

# Iterate through the list and delete each file
for file_name in file_list:
    file_path = os.path.join(pdb_folder, file_name)
    os.remove(file_path)

cur.close()
conn.close()