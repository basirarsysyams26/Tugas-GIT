import pandas as pd

class MarketingDataETL:
    def __init__(self, filename):
        self.filename = filename
        self.data = None
    
    def extract(self):
        try:
            self.data = pd.read_csv(self.filename, sep=';')
            print("Data berhasil diekstrak dari file:", self.filename)
        except FileNotFoundError:
            print("File tidak ditemukan.")
        except Exception as e:
            print("Terjadi kesalahan saat ekstraksi data:", str(e))
    
    def transform(self):
        if self.data is not None:
            # Implementasi transformasi sederhana pada data
            try:
                # Mengubah format tanggal
                self.data['purchase_date'] = pd.to_datetime(self.data['purchase_date'], errors='coerce', format='%d/%m/%y')
                print("Data berhasil ditransformasi.")
            except Exception as e:
                print("Terjadi kesalahan saat transformasi data:", str(e))
        else:
            print("Data belum diekstrak. Lakukan ekstraksi terlebih dahulu.")
    
    def store(self, output_filename):
        if self.data is not None:
            # Menyimpan data yang telah ditransformasi ke dalam DataFrame
            try:
                self.data.to_csv(output_filename, index=False)
                print("Data berhasil disimpan dalam file:", output_filename)
            except Exception as e:
                print("Terjadi kesalahan saat penyimpanan data:", str(e))
        else:
            print("Data belum diekstrak dan ditransformasi. Lakukan ekstraksi dan transformasi terlebih dahulu.")

# Contoh penggunaan
if __name__ == "__main__":
    etl = MarketingDataETL("marketing_data.csv")
    etl.extract()
    etl.transform()
    etl.store("transformed_marketing_data.csv")
