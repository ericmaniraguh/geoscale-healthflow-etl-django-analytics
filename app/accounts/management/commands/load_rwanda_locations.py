# app/accounts/management/commands/load_rwanda_locations.py
from django.core.management.base import BaseCommand
from django.apps import apps

PROVINCES = [
    "Northern", "Eastern", "Southern", "Western", "City of Kigali",
]

DISTRICTS = {
    "City of Kigali": ["Gasabo", "Kicukiro", "Nyarugenge"],
    "Eastern": ["Bugesera", "Gatsibo", "Kayonza", "Kirehe", "Ngoma", "Nyagatare", "Rwamagana"],
    "Northern": ["Burera", "Gakenke", "Gicumbi", "Musanze", "Rulindo"],
    "Southern": ["Gisagara", "Huye", "Kamonyi", "Muhanga", "Nyamagabe", "Nyanza", "Nyaruguru", "Ruhango"],
    "Western": ["Karongi", "Ngororero", "Nyamasheke", "Nyabihu", "Rubavu", "Rusizi", "Rutsiro"],
}

SECTORS = {
    # City of Kigali
    ("City of Kigali", "Gasabo"): [
        "Bumbogo","Gatsata","Gikomero","Gisozi","Jabana","Jali",
        "Kacyiru","Kimihurura","Kimironko","Kinyinya","Ndera",
        "Nduba","Remera","Rusororo","Rutunga",
    ],
    ("City of Kigali", "Kicukiro"): [
        "Kicukiro","Gatenga","Gikondo","Kanombe","Masaka",
        "Nyarugunga","Gahanga","Kagarama","Niboye","Bicumbi",
    ],
    ("City of Kigali", "Nyarugenge"): [
        "Gitega","Kanyinya","Kigali","Kimisagara","Mageragere",
        "Muhima","Nyakabanda","Nyamirambo","Nyarugenge","Rwezamenyo",
    ],

    # Eastern
    ("Eastern", "Bugesera"): [
        "Gashora","Juru","Kamabuye","Ntarama","Mareba","Mayange",
        "Musenyi","Mwogo","Ngeruka","Nyamata","Nyarugenge","Rilima",
        "Ruhuha","Rweru","Shyara",
    ],
    ("Eastern", "Gatsibo"): [
        "Gasange","Gatsibo","Gitoki","Kabarore","Kageyo","Kiramuruzi",
        "Kiziguro","Muhura","Murambi","Ngarama","Nyagihanga","Remera",
        "Rugarama","Rwimbogo",
    ],
    ("Eastern", "Kayonza"): [
        "Gahini","Kabare","Kabarondo","Mukarange","Murama","Murundi",
        "Mwiri","Ndego","Nyamirama","Rukara","Ruramira","Rwinkwavu",
    ],
    ("Eastern", "Kirehe"): [
        "Gahara","Gatore","Kigarama","Kigina","Kirehe","Mahama",
        "Mpanga","Musaza","Mushikiri","Nasho","Nyamugari","Nyarubuye",
    ],
    ("Eastern", "Ngoma"): [
        "Gashanda","Jarama","Karembo","Kazo","Kibungo","Mugesera",
        "Murama","Mutenderi","Remera","Rukira","Rukumberi","Rurenge",
        "Sake","Zaza",
    ],
    ("Eastern", "Nyagatare"): [
        "Gatunda","Karama","Karangazi","Katabagemu","Kiyombe","Matimba",
        "Mimuli","Mukama","Musheli","Nyagatare","Rukomo","Rwempasha",
        "Rwimiyaga","Tabagwe",
    ],
    ("Eastern", "Rwamagana"): [
        "Fumbwe","Gahengeri","Gishali","Karenge","Kigabiro","Muhazi",
        "Munyaga","Munyiginya","Musha","Muyumbu","Mwulire","Nyakariro",
        "Nzige","Rubona",
    ],

    # Northern
    ("Northern", "Burera"): [
        "Bungwe","Butaro","Cyanika","Cyeru","Gahunga","Gatebe",
        "Gitovu","Kagogo","Kinoni","Kinyababa","Kivuye","Nemba",
        "Rugarama","Rugendabari","Ruhunde","Rusarabuye","Rwerere",
    ],
    ("Northern", "Gakenke"): [
        "Busengo","Coko","Cyabingo","Gakenke","Gashenyi","Mugunga",
        "Janja","Kamubuga","Karambo","Kivuruga","Mataba","Minazi",
        "Muhondo","Muyongwe","Muzo","Nemba","Ruli","Rusasa","Rushashi",
    ],
    ("Northern", "Gicumbi"): [
        "Bukure","Bwisige","Byumba","Cyumba","Giti","Kaniga",
        "Manyagiro","Miyove","Kageyo","Mukarange","Muko","Mutete",
        "Nyamiyaga","Nyankenke II","Rubaya","Rukomo","Rushaki",
        "Rutare","Ruvune","Rwamiko",
    ],
    ("Northern", "Musanze"): [
        "Busogo","Cyuve","Gacaca","Gashaki","Gataraga","Kimonyi",
        "Kinigi","Muhoza","Muko","Musanze","Nkotsi","Nyange",
        "Remera","Rwaza","Shingiro",
    ],
    ("Northern", "Rulindo"): [
        "Base","Burega","Bushoki","Buyoga","Cyinzuzi","Cyungo",
        "Kinihira","Kisaro","Masoro","Mbogo","Murambi","Ngoma",
        "Ntarabana","Rukozo","Rusiga","Shyorongi","Tumba",
    ],

    # Southern
    ("Southern", "Gisagara"): [
        "Gikonko","Gishubi","Kansi","Kibilizi","Kigembe","Mamba",
        "Muganza","Mugombwa","Mukindo","Musha","Ndora","Nyanza",
        "Save",
    ],
    ("Southern", "Huye"): [
        "Gishamvu","Karama","Kigoma","Kinazi","Maraba","Mbazi",
        "Mukura","Ngoma","Ruhashya","Huye","Rusatira","Rwaniro",
        "Simbi","Tumba",
        ],
    ("Southern", "Kamonyi"): [
        "Gacurabwenge","Karama","Kayenzi","Kayumbu","Mugina","Musambira",
        "Ngamba","Nyamiyaga","Nyarubaka","Rugalika","Rukoma","Runda",
    ],
    ("Southern", "Muhanga"): [
        "Cyeza","Nyabinoni","Nyamabuye","Rugendabari","Shyogwe",
    ],
    ("Southern", "Nyamagabe"): [
        "Buruhukiro","Cyanika","Gatare","Kaduha","Kamegeli","Kibirizi",
        "Kibumbwe","Kitabi","Mbazi","Mugano","Musange","Musebeya",
        "Mushubi","Nkomane","Gasaka","Tare","Uwinkingi",
    ],
    ("Southern", "Nyanza"): [
        "Busasamana","Busoro","Cyabakamyi","Kibirizi","Kigoma","Mukingo",
        "Muyira","Ntyazo","Nyagisozi","Rwabicuma",
    ],
    ("Southern", "Nyaruguru"): [
        "Cyahinda","Busanze","Kibeho","Mata","Munini","Kivu",
        "Ngera","Ngoma","Nyabimata","Nyagisozi","Muganza","Ruheru",
        "Ruramba","Rusenge",
    ],
    ("Southern", "Ruhango"): [
        "Kinazi","Byimana","Bweramana","Mbuye","Ruhango","Mwendo",
        "Kinihira","Ntongwe","Kabagari",
    ],

    # Western
    ("Western", "Karongi"): [
        "Bwishyura","Mutuntu","Rubengera","Gitesi","Ruganda","Rugabano",
        "Gishyita","Gishari","Mubuga","Murambi","Murundi","Rwankuba",
        "Twumba",
    ],
    ("Western", "Ngororero"): [
        "Bwira","Gatumba","Hindiro","Kabaya","Kageyo","Kavumu",
        "Matyazo","Muhanda","Muhororo","Ndaro","Ngororero","Nyange",
        "Sovu",
    ],
    ("Western", "Nyamasheke"): [
        "Ruharambuga","Bushekeri","Bushenge","Cyato","Gihombo","Kagano",
        "Kanjongo","Karambi","Karengera","Kirimbi","Macuba","Nyabitekeri",
        "Mahembe","Rangiro","Shangi",
    ],
    ("Western", "Nyabihu"): [
        "Bigogwe","Jenda","Jomba","Kabatwa","Karago","Kintobo",
        "Mukamira","Muringa","Rambura","Rugera","Rurembo","Shyira",
    ],
    ("Western", "Rubavu"): [
        "Gisenyi","Kanama","Kanzenze","Mudende","Nyakiriba","Nyamyumba",
        "Rubavu","Rugero","Cyanzarwe","Busasamana","Nyundo","Bugeshi",
    ],
    ("Western", "Rusizi"): [
        "Bugarama","Butare","Bweyeye","Gikundamvura","Gashonga","Giheke",
        "Gihundwe","Gitambi","Kamembe","Muganza","Mururu","Nkanka",
        "Nkombo","Nkungu","Nyakabuye","Nyakarenzo","Nzahaha","Rwimbogo",
    ],
    ("Western", "Rutsiro"): [
        "Boneza","Gihango","Kigeyo","Kivumu","Manihira","Mukura",
        "Murunda","Musasa","Mushonyi","Mushubati","Nyabirasi","Ruhango",
        "Rusebeya",
    ],
}

class Command(BaseCommand):
    help = "Seed Rwanda Provinces, Districts, and all Sectors."

    def handle(self, *args, **options):
        # Get models via app registry (works even when module path is app.accounts)
        Province = apps.get_model("accounts", "Province")
        District = apps.get_model("accounts", "District")
        Sector   = apps.get_model("accounts", "Sector")

        # Provinces
        p_objs = {p: Province.objects.get_or_create(name=p)[0] for p in PROVINCES}
        self.stdout.write(self.style.SUCCESS(f"Provinces upserted: {len(p_objs)}"))

        # Districts
        d_objs = {}
        for prov, dlist in DISTRICTS.items():
            for d in dlist:
                d_objs[(prov, d)] = District.objects.get_or_create(province=p_objs[prov], name=d)[0]
        self.stdout.write(self.style.SUCCESS(f"Districts upserted: {len(d_objs)}"))

        # Sectors
        count = 0
        for (prov, dist), slist in SECTORS.items():
            d_obj = d_objs[(prov, dist)]
            for s in slist:
                Sector.objects.get_or_create(district=d_obj, name=s)
                count += 1
        self.stdout.write(self.style.SUCCESS(f"Sectors upserted: {count}"))
        self.stdout.write(self.style.SUCCESS("Done."))
