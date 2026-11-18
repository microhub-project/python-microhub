import os

folder = 'data'
files = os.listdir(folder)

for f in files:
    filename = f.split('.')
    os.rename(f'{folder}/{f}', f'{folder}/{filename[0].upper().replace(':', '_')}.{filename[1]}')