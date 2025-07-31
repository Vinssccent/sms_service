# -*- coding: utf-8 -*-
# Финальная, полная версия сгенерированного файла.
import sys
sys.path.append('.')
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pydantic_settings import BaseSettings, SettingsConfigDict

class SeedSettings(BaseSettings):
    DB_USER: str; DB_PASSWORD: str; DB_NAME: str; DB_HOST: str = "db"
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+psycopg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}/{self.DB_NAME}"
    model_config = SettingsConfigDict(env_file='.env')

settings = SeedSettings()
engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

from src.models import Country, Service

# --- ПОЛНЫЙ СПИСОК СТРАН С РЕАЛЬНЫМИ ДАННЫМИ ---
COUNTRIES_DATA = [
    {'id': 0, 'name': 'Россия', 'iso_code': 'RU', 'phone_code': '7'},
    {'id': 1, 'name': 'Украина', 'iso_code': 'UA', 'phone_code': '380'},
    {'id': 2, 'name': 'Казахстан', 'iso_code': 'KZ', 'phone_code': '7'},
    {'id': 3, 'name': 'Китай', 'iso_code': 'CN', 'phone_code': '86'},
    {'id': 4, 'name': 'Филиппины', 'iso_code': 'PH', 'phone_code': '63'},
    {'id': 5, 'name': 'Мьянма', 'iso_code': 'MM', 'phone_code': '95'},
    {'id': 6, 'name': 'Индонезия', 'iso_code': 'ID', 'phone_code': '62'},
    {'id': 7, 'name': 'Малайзия', 'iso_code': 'MY', 'phone_code': '60'},
    {'id': 8, 'name': 'Кения', 'iso_code': 'KE', 'phone_code': '254'},
    {'id': 9, 'name': 'Танзания', 'iso_code': 'TZ', 'phone_code': '255'},
    {'id': 10, 'name': 'Вьетнам', 'iso_code': 'VN', 'phone_code': '84'},
    {'id': 11, 'name': 'Кыргызстан', 'iso_code': 'KG', 'phone_code': '996'},
    {'id': 13, 'name': 'Израиль', 'iso_code': 'IL', 'phone_code': '972'},
    {'id': 14, 'name': 'Гонконг', 'iso_code': 'HK', 'phone_code': '852'},
    {'id': 15, 'name': 'Польша', 'iso_code': 'PL', 'phone_code': '48'},
    {'id': 16, 'name': 'Великобритания', 'iso_code': 'GB', 'phone_code': '44'},
    {'id': 17, 'name': 'Мадагаскар', 'iso_code': 'MG', 'phone_code': '261'},
    {'id': 18, 'name': 'Дем. Конго', 'iso_code': 'CD', 'phone_code': '243'},
    {'id': 19, 'name': 'Нигерия', 'iso_code': 'NG', 'phone_code': '234'},
    {'id': 20, 'name': 'Макао', 'iso_code': 'MO', 'phone_code': '853'},
    {'id': 21, 'name': 'Египет', 'iso_code': 'EG', 'phone_code': '20'},
    {'id': 22, 'name': 'Индия', 'iso_code': 'IN', 'phone_code': '91'},
    {'id': 23, 'name': 'Ирландия', 'iso_code': 'IE', 'phone_code': '353'},
    {'id': 24, 'name': 'Камбоджа', 'iso_code': 'KH', 'phone_code': '855'},
    {'id': 25, 'name': 'Лаос', 'iso_code': 'LA', 'phone_code': '856'},
    {'id': 26, 'name': 'Гаити', 'iso_code': 'HT', 'phone_code': '509'},
    {'id': 27, 'name': "Кот д'Ивуар", 'iso_code': 'CI', 'phone_code': '225'},
    {'id': 28, 'name': 'Гамбия', 'iso_code': 'GM', 'phone_code': '220'},
    {'id': 29, 'name': 'Сербия', 'iso_code': 'RS', 'phone_code': '381'},
    {'id': 30, 'name': 'Йемен', 'iso_code': 'YE', 'phone_code': '967'},
    {'id': 31, 'name': 'ЮАР', 'iso_code': 'ZA', 'phone_code': '27'},
    {'id': 32, 'name': 'Румыния', 'iso_code': 'RO', 'phone_code': '40'},
    {'id': 33, 'name': 'Колумбия', 'iso_code': 'CO', 'phone_code': '57'},
    {'id': 34, 'name': 'Эстония', 'iso_code': 'EE', 'phone_code': '372'},
    {'id': 35, 'name': 'Азербайджан', 'iso_code': 'AZ', 'phone_code': '994'},
    {'id': 36, 'name': 'Канада', 'iso_code': 'CA', 'phone_code': '1'},
    {'id': 37, 'name': 'Марокко', 'iso_code': 'MA', 'phone_code': '212'},
    {'id': 38, 'name': 'Гана', 'iso_code': 'GH', 'phone_code': '233'},
    {'id': 39, 'name': 'Аргентина', 'iso_code': 'AR', 'phone_code': '54'},
    {'id': 40, 'name': 'Узбекистан', 'iso_code': 'UZ', 'phone_code': '998'},
    {'id': 41, 'name': 'Камерун', 'iso_code': 'CM', 'phone_code': '237'},
    {'id': 42, 'name': 'Чад', 'iso_code': 'TD', 'phone_code': '235'},
    {'id': 43, 'name': 'Германия', 'iso_code': 'DE', 'phone_code': '49'},
    {'id': 44, 'name': 'Литва', 'iso_code': 'LT', 'phone_code': '370'},
    {'id': 45, 'name': 'Хорватия', 'iso_code': 'HR', 'phone_code': '385'},
    {'id': 46, 'name': 'Швеция', 'iso_code': 'SE', 'phone_code': '46'},
    {'id': 47, 'name': 'Ирак', 'iso_code': 'IQ', 'phone_code': '964'},
    {'id': 48, 'name': 'Нидерланды', 'iso_code': 'NL', 'phone_code': '31'},
    {'id': 49, 'name': 'Латвия', 'iso_code': 'LV', 'phone_code': '371'},
    {'id': 50, 'name': 'Австрия', 'iso_code': 'AT', 'phone_code': '43'},
    {'id': 51, 'name': 'Беларусь', 'iso_code': 'BY', 'phone_code': '375'},
    {'id': 52, 'name': 'Таиланд', 'iso_code': 'TH', 'phone_code': '66'},
    {'id': 53, 'name': 'Сауд. Аравия', 'iso_code': 'SA', 'phone_code': '966'},
    {'id': 54, 'name': 'Мексика', 'iso_code': 'MX', 'phone_code': '52'},
    {'id': 55, 'name': 'Тайвань', 'iso_code': 'TW', 'phone_code': '886'},
    {'id': 56, 'name': 'Испания', 'iso_code': 'ES', 'phone_code': '34'},
    {'id': 57, 'name': 'Иран', 'iso_code': 'IR', 'phone_code': '98'},
    {'id': 58, 'name': 'Алжир', 'iso_code': 'DZ', 'phone_code': '213'},
    {'id': 59, 'name': 'Словения', 'iso_code': 'SI', 'phone_code': '386'},
    {'id': 60, 'name': 'Бангладеш', 'iso_code': 'BD', 'phone_code': '880'},
    {'id': 61, 'name': 'Сенегал', 'iso_code': 'SN', 'phone_code': '221'},
    {'id': 62, 'name': 'Турция', 'iso_code': 'TR', 'phone_code': '90'},
    {'id': 63, 'name': 'Чехия', 'iso_code': 'CZ', 'phone_code': '420'},
    {'id': 64, 'name': 'Шри-Ланка', 'iso_code': 'LK', 'phone_code': '94'},
    {'id': 65, 'name': 'Перу', 'iso_code': 'PE', 'phone_code': '51'},
    {'id': 66, 'name': 'Пакистан', 'iso_code': 'PK', 'phone_code': '92'},
    {'id': 67, 'name': 'Новая Зеландия', 'iso_code': 'NZ', 'phone_code': '64'},
    {'id': 68, 'name': 'Гвинея', 'iso_code': 'GN', 'phone_code': '224'},
    {'id': 69, 'name': 'Мали', 'iso_code': 'ML', 'phone_code': '223'},
    {'id': 70, 'name': 'Венесуэла', 'iso_code': 'VE', 'phone_code': '58'},
    {'id': 71, 'name': 'Эфиопия', 'iso_code': 'ET', 'phone_code': '251'},
    {'id': 72, 'name': 'Монголия', 'iso_code': 'MN', 'phone_code': '976'},
    {'id': 73, 'name': 'Бразилия', 'iso_code': 'BR', 'phone_code': '55'},
    {'id': 74, 'name': 'Афганистан', 'iso_code': 'AF', 'phone_code': '93'},
    {'id': 75, 'name': 'Уганда', 'iso_code': 'UG', 'phone_code': '256'},
    {'id': 76, 'name': 'Ангола', 'iso_code': 'AO', 'phone_code': '244'},
    {'id': 77, 'name': 'Кипр', 'iso_code': 'CY', 'phone_code': '357'},
    {'id': 78, 'name': 'Франция', 'iso_code': 'FR', 'phone_code': '33'},
    {'id': 79, 'name': 'Папуа-Новая Гвинея', 'iso_code': 'PG', 'phone_code': '675'},
    {'id': 80, 'name': 'Мозамбик', 'iso_code': 'MZ', 'phone_code': '258'},
    {'id': 81, 'name': 'Непал', 'iso_code': 'NP', 'phone_code': '977'},
    {'id': 82, 'name': 'Бельгия', 'iso_code': 'BE', 'phone_code': '32'},
    {'id': 83, 'name': 'Болгария', 'iso_code': 'BG', 'phone_code': '359'},
    {'id': 84, 'name': 'Венгрия', 'iso_code': 'HU', 'phone_code': '36'},
    {'id': 85, 'name': 'Молдова', 'iso_code': 'MD', 'phone_code': '373'},
    {'id': 86, 'name': 'Италия', 'iso_code': 'IT', 'phone_code': '39'},
    {'id': 87, 'name': 'Парагвай', 'iso_code': 'PY', 'phone_code': '595'},
    {'id': 88, 'name': 'Гондурас', 'iso_code': 'HN', 'phone_code': '504'},
    {'id': 89, 'name': 'Тунис', 'iso_code': 'TN', 'phone_code': '216'},
    {'id': 90, 'name': 'Никарагуа', 'iso_code': 'NI', 'phone_code': '505'},
    {'id': 91, 'name': 'Тимор-Лесте', 'iso_code': 'TL', 'phone_code': '670'},
    {'id': 92, 'name': 'Боливия', 'iso_code': 'BO', 'phone_code': '591'},
    {'id': 93, 'name': 'Коста Рика', 'iso_code': 'CR', 'phone_code': '506'},
    {'id': 94, 'name': 'Гватемала', 'iso_code': 'GT', 'phone_code': '502'},
    {'id': 95, 'name': 'ОАЭ', 'iso_code': 'AE', 'phone_code': '971'},
    {'id': 96, 'name': 'Зимбабве', 'iso_code': 'ZW', 'phone_code': '263'},
    {'id': 97, 'name': 'Пуэрто-Рико', 'iso_code': 'PR', 'phone_code': '1'},
    {'id': 98, 'name': 'Судан', 'iso_code': 'SD', 'phone_code': '249'},
    {'id': 99, 'name': 'Того', 'iso_code': 'TG', 'phone_code': '228'},
    {'id': 100, 'name': 'Кувейт', 'iso_code': 'KW', 'phone_code': '965'},
    {'id': 101, 'name': 'Сальвадор', 'iso_code': 'SV', 'phone_code': '503'},
    {'id': 102, 'name': 'Ливия', 'iso_code': 'LY', 'phone_code': '218'},
    {'id': 103, 'name': 'Ямайка', 'iso_code': 'JM', 'phone_code': '1'},
    {'id': 104, 'name': 'Тринидад и Тобаго', 'iso_code': 'TT', 'phone_code': '1'},
    {'id': 105, 'name': 'Эквадор', 'iso_code': 'EC', 'phone_code': '593'},
    {'id': 106, 'name': 'Свазиленд', 'iso_code': 'SZ', 'phone_code': '268'},
    {'id': 107, 'name': 'Оман', 'iso_code': 'OM', 'phone_code': '968'},
    {'id': 108, 'name': 'Босния и Герцеговина', 'iso_code': 'BA', 'phone_code': '387'},
    {'id': 109, 'name': 'Доминиканская Республика', 'iso_code': 'DO', 'phone_code': '1'},
    {'id': 110, 'name': 'Сирия', 'iso_code': 'SY', 'phone_code': '963'},
    {'id': 111, 'name': 'Катар', 'iso_code': 'QA', 'phone_code': '974'},
    {'id': 112, 'name': 'Панама', 'iso_code': 'PA', 'phone_code': '507'},
    {'id': 113, 'name': 'Куба', 'iso_code': 'CU', 'phone_code': '53'},
    {'id': 114, 'name': 'Мавритания', 'iso_code': 'MR', 'phone_code': '222'},
    {'id': 115, 'name': 'Сьерра-Леоне', 'iso_code': 'SL', 'phone_code': '232'},
    {'id': 116, 'name': 'Иордания', 'iso_code': 'JO', 'phone_code': '962'},
    {'id': 117, 'name': 'Португалия', 'iso_code': 'PT', 'phone_code': '351'},
    {'id': 118, 'name': 'Барбадос', 'iso_code': 'BB', 'phone_code': '1'},
    {'id': 119, 'name': 'Бурунди', 'iso_code': 'BI', 'phone_code': '257'},
    {'id': 120, 'name': 'Бенин', 'iso_code': 'BJ', 'phone_code': '229'},
    {'id': 121, 'name': 'Бруней', 'iso_code': 'BN', 'phone_code': '673'},
    {'id': 122, 'name': 'Багамы', 'iso_code': 'BS', 'phone_code': '1'},
    {'id': 123, 'name': 'Ботсвана', 'iso_code': 'BW', 'phone_code': '267'},
    {'id': 124, 'name': 'Белиз', 'iso_code': 'BZ', 'phone_code': '501'},
    {'id': 125, 'name': 'ЦАР', 'iso_code': 'CF', 'phone_code': '236'},
    {'id': 126, 'name': 'Доминика', 'iso_code': 'DM', 'phone_code': '1'},
    {'id': 127, 'name': 'Гренада', 'iso_code': 'GD', 'phone_code': '1'},
    {'id': 128, 'name': 'Грузия', 'iso_code': 'GE', 'phone_code': '995'},
    {'id': 129, 'name': 'Греция', 'iso_code': 'GR', 'phone_code': '30'},
    {'id': 130, 'name': 'Гвинея-Бисау', 'iso_code': 'GW', 'phone_code': '245'},
    {'id': 131, 'name': 'Гайана', 'iso_code': 'GY', 'phone_code': '592'},
    {'id': 132, 'name': 'Исландия', 'iso_code': 'IS', 'phone_code': '354'},
    {'id': 133, 'name': 'Коморы', 'iso_code': 'KM', 'phone_code': '269'},
    {'id': 134, 'name': 'Сент-Китс и Невис', 'iso_code': 'KN', 'phone_code': '1'},
    {'id': 135, 'name': 'Либерия', 'iso_code': 'LR', 'phone_code': '231'},
    {'id': 136, 'name': 'Лесото', 'iso_code': 'LS', 'phone_code': '266'},
    {'id': 137, 'name': 'Малави', 'iso_code': 'MW', 'phone_code': '265'},
    {'id': 138, 'name': 'Намибия', 'iso_code': 'NA', 'phone_code': '264'},
    {'id': 139, 'name': 'Нигер', 'iso_code': 'NE', 'phone_code': '227'},
    {'id': 140, 'name': 'Руанда', 'iso_code': 'RW', 'phone_code': '250'},
    {'id': 141, 'name': 'Словакия', 'iso_code': 'SK', 'phone_code': '421'},
    {'id': 142, 'name': 'Суринам', 'iso_code': 'SR', 'phone_code': '597'},
    {'id': 143, 'name': 'Таджикистан', 'iso_code': 'TJ', 'phone_code': '992'},
    {'id': 144, 'name': 'Монако', 'iso_code': 'MC', 'phone_code': '377'},
    {'id': 145, 'name': 'Бахрейн', 'iso_code': 'BH', 'phone_code': '973'},
    {'id': 146, 'name': 'Реюньон', 'iso_code': 'RE', 'phone_code': '262'},
    {'id': 147, 'name': 'Замбия', 'iso_code': 'ZM', 'phone_code': '260'},
    {'id': 148, 'name': 'Армения', 'iso_code': 'AM', 'phone_code': '374'},
    {'id': 149, 'name': 'Сомали', 'iso_code': 'SO', 'phone_code': '252'},
    {'id': 150, 'name': 'Конго', 'iso_code': 'CG', 'phone_code': '242'},
    {'id': 151, 'name': 'Чили', 'iso_code': 'CL', 'phone_code': '56'},
    {'id': 152, 'name': 'Буркина-Фасо', 'iso_code': 'BF', 'phone_code': '226'},
    {'id': 153, 'name': 'Ливан', 'iso_code': 'LB', 'phone_code': '961'},
    {'id': 154, 'name': 'Габон', 'iso_code': 'GA', 'phone_code': '241'},
    {'id': 155, 'name': 'Албания', 'iso_code': 'AL', 'phone_code': '355'},
    {'id': 156, 'name': 'Уругвай', 'iso_code': 'UY', 'phone_code': '598'},
    {'id': 157, 'name': 'Маврикий', 'iso_code': 'MU', 'phone_code': '230'},
    {'id': 158, 'name': 'Бутан', 'iso_code': 'BT', 'phone_code': '975'},
    {'id': 159, 'name': 'Мальдивы', 'iso_code': 'MV', 'phone_code': '960'},
    {'id': 160, 'name': 'Гваделупа', 'iso_code': 'GP', 'phone_code': '590'},
    {'id': 161, 'name': 'Туркменистан', 'iso_code': 'TM', 'phone_code': '993'},
    {'id': 162, 'name': 'Французская Гвиана', 'iso_code': 'GF', 'phone_code': '594'},
    {'id': 163, 'name': 'Финляндия', 'iso_code': 'FI', 'phone_code': '358'},
    {'id': 164, 'name': 'Сент-Люсия', 'iso_code': 'LC', 'phone_code': '1'},
    {'id': 165, 'name': 'Люксембург', 'iso_code': 'LU', 'phone_code': '352'},
    {'id': 166, 'name': 'Сент-Винсент и Гренадин', 'iso_code': 'VC', 'phone_code': '1'},
    {'id': 167, 'name': 'Экваториальная Гвинея', 'iso_code': 'GQ', 'phone_code': '240'},
    {'id': 168, 'name': 'Джибути', 'iso_code': 'DJ', 'phone_code': '253'},
    {'id': 169, 'name': 'Антигуа и Барбуда', 'iso_code': 'AG', 'phone_code': '1'},
    {'id': 170, 'name': 'Острова Кайман', 'iso_code': 'KY', 'phone_code': '1'},
    {'id': 171, 'name': 'Черногория', 'iso_code': 'ME', 'phone_code': '382'},
    {'id': 172, 'name': 'Дания', 'iso_code': 'DK', 'phone_code': '45'},
    {'id': 173, 'name': 'Швейцария', 'iso_code': 'CH', 'phone_code': '41'},
    {'id': 174, 'name': 'Норвегия', 'iso_code': 'NO', 'phone_code': '47'},
    {'id': 175, 'name': 'Австралия', 'iso_code': 'AU', 'phone_code': '61'},
    {'id': 176, 'name': 'Эритрея', 'iso_code': 'ER', 'phone_code': '291'},
    {'id': 177, 'name': 'Южный Судан', 'iso_code': 'SS', 'phone_code': '211'},
    {'id': 178, 'name': 'Сан-Томе и Принсипи', 'iso_code': 'ST', 'phone_code': '239'},
    {'id': 179, 'name': 'Аруба', 'iso_code': 'AW', 'phone_code': '297'},
    {'id': 180, 'name': 'Монтсеррат', 'iso_code': 'MS', 'phone_code': '1'},
    {'id': 181, 'name': 'Ангилья', 'iso_code': 'AI', 'phone_code': '1'},
    {'id': 182, 'name': 'Япония', 'iso_code': 'JP', 'phone_code': '81'},
    {'id': 183, 'name': 'Северная Македония', 'iso_code': 'MK', 'phone_code': '389'},
    {'id': 184, 'name': 'Республика Сейшелы', 'iso_code': 'SC', 'phone_code': '248'},
    {'id': 185, 'name': 'Новая Каледония', 'iso_code': 'NC', 'phone_code': '687'},
    {'id': 186, 'name': 'Кабо-Верде', 'iso_code': 'CV', 'phone_code': '238'},
    {'id': 188, 'name': 'Палестина', 'iso_code': 'PS', 'phone_code': '970'},
    {'id': 189, 'name': 'Фиджи', 'iso_code': 'FJ', 'phone_code': '679'},
    {'id': 196, 'name': 'Сингапур', 'iso_code': 'SG', 'phone_code': '65'},
    {'id': 199, 'name': 'Мальта', 'iso_code': 'MT', 'phone_code': '356'},
    {'id': 201, 'name': 'Гибралтар', 'iso_code': 'GI', 'phone_code': '350'},
    {'id': 203, 'name': 'Косово', 'iso_code': 'XK', 'phone_code': '383'},
    {'id': 204, 'name': 'Ниуэ', 'iso_code': 'NU', 'phone_code': '683'},
]

def seed_data():
    db = SessionLocal()
    try:
        # Seed Countries
        if db.query(Country).count() == 0:
            print("Seeding Countries...")
            countries_to_add = [Country(**data) for data in COUNTRIES_DATA]
            db.bulk_save_objects(countries_to_add)
            db.commit()
            print("SUCCESS: Countries seeded.")
        
        # Seed Services
        if db.query(Service).count() == 0:
            print("Seeding services from data/services.json...")
            with open('data/services.json', 'r', encoding='utf-8') as f:
                services_data = json.load(f)
            
            services_to_add = []
            added_codes = set(); added_names = set()
            for item in services_data:
                code, name = item.get("code"), item.get("name")
                if code and name and code not in added_codes and name not in added_names:
                    services_to_add.append(Service(name=name, code=code))
                    added_codes.add(code); added_names.add(name)
            
            db.bulk_save_objects(services_to_add)
            db.commit()
            print("SUCCESS: Services seeded from JSON.")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        db.rollback()
    finally:
        db.close()
            
if __name__ == "__main__":
    print("Starting DB Seeding...")
    seed_data()
    print("DB Seeding Finished.")
