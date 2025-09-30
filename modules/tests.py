from lists import list_

read_path = '/Users/albertoruizcajiga/Library/CloudStorage/GoogleDrive-beautifulday874@gmail.com/My Drive/Information_Technology/alberto/utilities/to_process/alberto/apollo-contacts-export.csv'

df = list_.ReadList(read_path)
df = list_.FixColumns(df)

print(df.columns)