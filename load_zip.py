import requests
import zipfile
import os 
from .subfolder import print_nimporte_quoi

# Make a GET request to download the ZIP file
response = requests.get('https://blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/5df5dd/games_ratings.zip')

file = 'games_ratings.zip'

# Save the ZIP file to disk
with open(file, 'wb') as f:
    f.write(response.content)

# Extract the contents of the ZIP file
with zipfile.ZipFile(file, 'r') as zip_ref:
    zip_ref.extractall('games_ratings')

print_nimporte_quoi()

if os.path.exists(file):
    os.remove(file)
    print(f'File {file} deleted successfully.')