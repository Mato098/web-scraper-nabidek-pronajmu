import logging
from time import time
from urllib.parse import urljoin

import requests

from disposition import Disposition
from scrapers.rental_offer import RentalOffer
from scrapers.scraper_base import ScraperBase
from scrapers.rental_offer import RentalOffer
from time import time
import requests
from urllib.parse import urljoin


class ScraperSreality(ScraperBase):

    name = "Sreality"
    logo_url = "https://www.sreality.cz/img/icons/android-chrome-192x192.png"
    color = 0xCC0000
    base_url = "https://www.sreality.cz"

    disposition_mapping = {
        Disposition.FLAT_1KK: "2",
        Disposition.FLAT_1: "3",
        Disposition.FLAT_2KK: "4",
        Disposition.FLAT_2: "5",
        Disposition.FLAT_3KK: "6",
        Disposition.FLAT_3: "7",
        Disposition.FLAT_4KK: "8",
        Disposition.FLAT_4: "9",
        Disposition.FLAT_5_UP: ("10", "11", "12"),
        Disposition.FLAT_OTHERS: "16",
    }

    _category_type_to_url = {
        0: "vse",
        1: "prodej",
        2: "pronajem",
        3: "drazby"
    }

    _category_main_to_url = {
        0: "vse",
        1: "byt",
        2: "dum",
        3: "pozemek",
        4: "komercni",
        5: "ostatni"
    }

    _category_sub_to_url = {
            2: "1+kk",
            3: "1+1",
            4: "2+kk",
            5: "2+1",
            6: "3+kk",
            7: "3+1",
            8: "4+kk",
            9: "4+1",
            10: "5+kk",
            11: "5+1",
            12: "6-a-vice",
            16: "atypicky",
            47: "pokoj",
            37: "rodinny",
            39: "vila",
            43: "chalupa",
            33: "chata",
            35: "pamatka",
            40: "na-klic",
            44: "zemedelska-usedlost",
            19: "bydleni",
            18: "komercni",
            20: "pole",
            22: "louka",
            21: "les",
            46: "rybnik",
            48: "sady-vinice",
            23: "zahrada",
            24: "ostatni-pozemky",
            25: "kancelare",
            26: "sklad",
            27: "vyrobni-prostor",
            28: "obchodni-prostor",
            29: "ubytovani",
            30: "restaurace",
            31: "zemedelsky",
            38: "cinzovni-dum",
            49: "virtualni-kancelar",
            32: "ostatni-komercni-prostory",
            34: "garaz",
            52: "garazove-stani",
            50: "vinny-sklep",
            51: "pudni-prostor",
            53: "mobilni-domek",
            36: "jine-nemovitosti"
        }


    def get_latest_offers(self) -> list[RentalOffer]:
        url = self.base_url + "/hledani/pronajem/byty?region=okres-brno-mesto"
        logging.debug("Sreality request: %s", url)

        # Hitting the HTML page directly as the REST API is deprecated, 
        # extracting initial Next.js state
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        import re
        import json
        
        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">([^<]+)</script>', response.text)
        if not match:
            logging.error("Sreality request returned no __NEXT_DATA__")
            return []
            
        try:
            data = json.loads(match.group(1))
            queries = data['props']['pageProps']['dehydratedState']['queries']
        except (KeyError, json.JSONDecodeError):
            logging.error("Sreality request returned invalid __NEXT_DATA__ JSON structure")
            return []

        results = []
        for q in queries:
            if q['queryKey'][0] == 'estatesSearch':
                d = q.get('state', {}).get('data', {})
                results = d.get('results', [])
                break

        items: list[RentalOffer] = []
        allowed_dispositions = self.get_dispositions_data()

        for item in results:
            # Check dispositions manually since we fetch all dispositions in this query
            sub_cb_val = str(item.get("categorySubCb", {}).get("value", ""))
            if sub_cb_val not in allowed_dispositions:
                continue

            # Skip tips
            if item.get("regionTip", 0) > 0 or item.get("brokerTip", 0) > 0 or item.get("projectTip", 0) > 0:
                continue
                
            # Build locality string for URL
            loc = item.get("locality", {})
            parts = []
            for p in ["citySeoName", "cityPartSeoName", "streetSeoName"]:
                if loc.get(p):
                    parts.append(loc[p])
            locality_url = "-".join(parts) if parts else "lokalita"
            
            link = f"{self.base_url}/detail/pronajem/byt/{self._category_sub_to_url.get(int(sub_cb_val), 'jiny')}/{locality_url}/{item['id']}"

            # Location string for display
            loc_str = []
            if loc.get('street'):
                loc_str.append(loc['street'])
            elif loc.get('cityPart'):
                loc_str.append(loc['cityPart'])
            if loc.get('city'):
                loc_str.append(loc['city'])
            location_str = ", ".join(loc_str)

            image_url = ""
            images = item.get("images", [])
            if images:
                image_url = images[0].get("url", "")
                if image_url and image_url.startswith("//"):
                    image_url = "https:" + image_url

            items.append(RentalOffer(
                scraper = self,
                link = link,
                title = item.get("name", "Neznámý název"),
                location = location_str,
                price = item.get("priceCzk", 0),
                image_url = image_url
            ))

        return items
