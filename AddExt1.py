import pandas as pd
import glob

path =r"C:\Backup\Config" # use your path
all_files = glob.glob(path + "/*.txt")

for filename in all_files:
    if "_Variable_2020" in filename:
        with  open(filename, "r") as file:
            nonempty_lines = [line.strip("\n") for line in file if line != "\n"]
            line_count = len(nonempty_lines)
            if line_count < 16:
                continue
        
        df = pd.read_csv(filename,sep='\t', index_col=None, skiprows=13)
        ext = []
        for item in df['0']:
            if item.endswith('Timestamp'):
                ext.append('Worker.TimeStamp')
            elif " DM " in item or " FM " in item:
                ext.append('Worker.Bool')
            elif " VM " in item or " MV " in item or " TE " in item:
                ext.append('Worker.Sine')        
            else:           
                ext.append('')
                
        # Creating a column from the list
        df['20000'] = ext
        filename = filename.replace(r"C:\Backup\Config", r"C:\Backup\C2_out")
        df.to_csv(filename, sep="\t", index=False)        