import datetime
import sys
from pathlib import Path

from sqlmodel import Session, select

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.session import engine
from app.models import PlatformInfo


nuevos_logos = {
    "buda": "https://blog.buda.com/content/images/2025/04/buda-logo-white-1.svg",
    "bitso": "https://bitso.com/__next/_next/static/media/bitso.b09b228b.svg",
    "cryptomkt": "https://www.cryptomkt.com/static/landing/img/resources/logos/Logo-1.png",
    "binance": "https://bin.bnbstatic.com/static/images/common/favicon.ico",
    "lulox": "https://tawk.link/65295746eb150b3fb9a11be6/kb/logo/4WfzNB9oM3.png",
    "wenia": "https://pbs.twimg.com/profile_images/1778520926155407360/20JTWGH2_400x400.jpg",
    "global66": "https://www.global66.com/blog/wp-content/uploads/2022/03/logo_desktop.svg",
    "dolarapp": "https://www.arqfinance.com/favicon.svg",
    "plenti": "https://cdn.prod.website-files.com/6697e29d92e2b75be213df4c/669a8987bfcc824265f6195c_Logo-white.svg",
    "littio": "https://framerusercontent.com/images/mqmwc7ucueZ7kZOWhBS56Wb0vo.png",
}


def main() -> None:
    updated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with Session(engine) as session:
        for platform_id, logo_url in nuevos_logos.items():
            platform = session.exec(
                select(PlatformInfo).where(PlatformInfo.id == platform_id)
            ).first()

            if not platform:
                print(f"Skipping {platform_id}: platform not found")
                continue

            platform.logo_url = logo_url
            platform.last_updated = updated_at
            session.add(platform)
            print(f"Updated {platform_id}")

        session.commit()


if __name__ == "__main__":
    main()
