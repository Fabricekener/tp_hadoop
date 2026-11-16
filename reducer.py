#!/usr/bin/env python3
import sys
mot_courant = None
total = 0
for ligne in sys.stdin:
    mot, valeur = ligne.rstrip("\n").split("\t", 1)
    valeur = int(valeur)
    if mot == mot_courant:
        total += valeur
    else:
        if mot_courant is not None:
            print(f"{mot_courant}\t{total}")
        mot_courant = mot
        total = valeur
if mot_courant is not None:
    print(f"{mot_courant}\t{total}")
