input_file = "proxies/proxies.txt"
output_file = "proxies/proxies_https.txt"

with open(input_file, "r") as fin, open(output_file, "w") as fout:
    for line in fin:
        proxy = line.strip()
        if proxy:
            fout.write(f"https://{proxy}\n")

print("Đã convert xong. Hãy dùng proxies_http.txt cho crawler!")
