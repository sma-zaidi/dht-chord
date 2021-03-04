import hashlib

m = 8

def Hash(key):
	hcode = hashlib.sha1()
	hcode.update(str(key).encode())
	return (int(hcode.hexdigest(), 16) % 2**m)
	