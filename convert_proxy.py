input_file = "proxies/proxies.txt"
output_file = "proxies/proxies_http.txt"

with open(input_file, "r") as fin, open(output_file, "w") as fout:
    for line in fin:
        proxy = line.strip()
        if proxy:
            fout.write(f"http://{proxy}\n")

print("Đã convert xong. Hãy dùng proxies_http.txt cho crawler!")
