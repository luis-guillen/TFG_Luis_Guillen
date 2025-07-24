import pickle
import sys

print("Python version:", sys.version)

try:
    # Load the pickle file
    with open('/Users/luisguillen/Downloads/210309-BAM_OG/Data/bam_2/B66406935.pkl', 'rb') as f:
        data = pickle.load(f)

    # Print the type and contents
    print("\nTipo de datos:", type(data))
    print("\nContenido del archivo:")
    print(data)
except Exception as e:
    print("\nError al cargar el archivo:")
    print(str(e)) 