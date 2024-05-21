import yaml
import sys

with open(sys.argv[1], 'r') as file:
    data = yaml.safe_load(file)
# print(data)

proxies = data["proxies"]

glide_forward_file = sys.argv[2]

forwarders = []

for p in proxies:
    #print(p)
    if p["type"] != "ss": continue
    # template = f"forward={p['type']}://{p['cipher']}:{p['password']}@{p['server']}:{p['port']} #{p['name']}\n"
    template = f"forward={p['type']}://{p['cipher']}:{p['password']}@{p['server']}:{p['port']}\n"
    print(template)
    forwarders.append(template)


with open(glide_forward_file, "w") as f:
    f.writelines(forwarders)
    #for e in forwarders:
    #    f.writeline(e)

