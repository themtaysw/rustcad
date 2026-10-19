# J1939 29-bit CAN ID

J1939 používá 29bitové CAN ID. Jednotlivé bity znamenají tohle (bit 28 je nejvyšší):

```
bity:   28 27 26 | 25 | 24 | 23 ......... 16 | 15 .......... 8 | 7 ........... 0
        priorita | R  | DP | PF (PDU format) | PS (PDU spec.)  | SA (source addr)
         3 bity  |    |    |     8 bitů      |     8 bitů      |     8 bitů
```
- Priorita 0–7: nižší číslo znamená důležitější zprávu.
- SA: kdo zprávu poslal (motor má typicky adresu 0x00).
- PGN je číslo, které říká, „co je to za zprávu“. Skládá se z bitů R, DP, PF a PS. Pozor na pravidlo ze zadání:
- PF ≥ 240 (PDU2): zpráva je broadcast, PS je součást PGN.
- PF < 240 (PDU1): PS je adresa příjemce a do PGN se nepočítá (místo něj je tam 0).

Příklad:
```
0x0CF00400 je EEC1 (otáčky motoru):
0x0C = 0000 1100 → priorita = 0x0C >> 2 = 3,  R = 0, DP = 0
PF = 0xF0 (240)  → PDU2, takže PS patří do PGN
PS = 0x04,  SA = 0x00
PGN = 0xF004 = 61444 ✓ (sedí s tabulkou v zadání)
```