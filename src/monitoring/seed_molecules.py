from __future__ import annotations

from src.database.db import connect

# Canonical name + pipe-separated synonyms. PubMed search is case-insensitive.
MOLECULES = [
    ("guselkumab", "guselkumab|Gulsekumab"),
    ("Risankizumab", "Risankizumab|risankizumab"),
    ("Tildrakizumab", "Tildrakizumab|tildrakizumab"),
    ("Secukinumab", "Secukinumab|secukinumab"),
    ("Ixekizumab", "Ixekizumab|ixekizumab"),
    ("Brodalumab", "Brodalumab|brodalumab"),
    ("Ustekinumab", "Ustekinumab|ustekinumab"),
    ("Apremilast", "Apremilast|apremilast|Otezla|otezla"),
    ("Deucravacitinib", "Deucravacitinib|deucravacitinib|BMS-986165|Sotyktu"),
    ("Piclidenoson", "Piclidenoson|CF101|piclidenoson"),
    ("Zasocitinib", "Zasocitinib|zasocitinib"),
    ("Icotrokinra", "Icotrokinra|Icotyde|icotrokinra|icotyde|JNJ-2113|JNJ-77242113"),
    ("Envudeucitinib", "Envudeucitinib|envudeucitinib|ESK-001"),
    ("SFA-002", "SFA-002"),
    ("AX-158", "AX-158"),
    ("TLL018", "TLL018"),
    ("Simepdekinra", "Simepdekinra|DC-853|simepdekinra"),
    ("IRX4204", "IRX4204"),
    ("ORKA-001", "ORKA-001"),
    ("ORKA-002", "ORKA-002"),
    ("Xeligekimab", "Xeligekimab|xeligekimab"),
    ("Vunakizumab", "Vunakizumab|vunakizumab|SHR-1314"),
    ("Netakimab", "Netakimab|netakimab|Efleira|efleira"),
    ("Picankibart", "Picankibart|Pecondle|pecondle|IBI112|ibi112|picankibart"),
    ("Gumokimab", "Gumokimab|AK111|gumokimab"),
    ("608", "608"),
    ("Roconkibart", "Roconkibart|JS005|roconkibart"),
    ("LZM012", "LZM012"),
    ("HS-10374", "HS-10374"),
    ("ICP-488", "ICP-488"),
    ("HS-20137", "HS-20137|QX004N"),
    ("HB0017", "HB0017"),
    ("D-2570", "D-2570"),
    ("SYHX1901", "SYHX1901"),
    ("WD-890", "WD-890"),
    ("SHR-1139", "SHR-1139"),
    ("AC-201", "AC-201"),
    ("RSS0393", "RSS0393"),
    ("CS32582", "CS32582"),
    ("RAP-103", "RAP-103"),
    ("UA021", "UA021"),
    ("Soficitinib", "Soficitinib|ICP-332|soficitinib"),
    ("Rimegepant", "Rimegepant|Nurtec|nurtec|rimegepant"),
    ("Balinatunfib", "Balinatunfib|SAR441566"),
    ("ME3183", "ME3183"),
    ("Sonelokimab", "Sonelokimab|sonelokimab"),
    ("LY3972406", "LY3972406"),
    ("Spesolimab", "Spesolimab|spevigo|spesolimab|Spevigo"),
    ("Imsidolimab", "Imsidolimab|ANB09|imsidolimab"),
    ("TAK-279", "TAK-279|NDI-034858"),
    ("Delgocitinib", "Delgocitinib|delgocitinib"),
    ("HB0034", "HB0034"),
    ("TQH2929", "TQH2929"),
]


def main() -> None:
    with connect() as conn:
        for name, synonyms in MOLECULES:
            conn.execute(
                "INSERT OR IGNORE INTO molecules (name, synonyms) VALUES (?, ?)",
                (name, synonyms),
            )
            print(f"✅ molecule loaded: {name} ({synonyms})")


if __name__ == "__main__":
    main()
