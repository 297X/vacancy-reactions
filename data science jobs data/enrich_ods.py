import json
import re
import time
import urllib.request
from pathlib import Path

INPUT = Path("ods data/result.json")
OUTPUT = Path("ods data/result_enriched.json")

def fetch_vacancy(url: str) -> dict | None:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")
    except Exception as e:
        print(f"  ERROR fetching {url}: {e}")
        return None

    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        print(f"  ERROR: no __NEXT_DATA__ at {url}")
        return None

    try:
        data = json.loads(m.group(1))
        return data["props"]["pageProps"]["vacancy"]
    except Exception as e:
        print(f"  ERROR parsing JSON at {url}: {e}")
        return None


def vacancy_to_text(v: dict) -> str:
    parts = []

    def add(label: str, value: str):
        value = value.strip()
        if value:
            parts.append(f"**{label}**\n{value}")

    # Зарплата
    sal_from = v.get("salary_from")
    sal_to = v.get("salary_to")
    currency = {"RUB": "₽", "USD": "$", "EUR": "€"}.get(v.get("salary_currency", ""), "")
    period = {"Month": "/месяц", "Year": "/год"}.get(v.get("salary_payment_period", ""), "")
    if sal_from or sal_to:
        salary = f"{sal_from or ''} – {sal_to or ''}".strip(" –")
        parts.append(f"**Зарплата:** {salary} {currency}{period}".strip())

    # Формат и уровень
    work_type = {"Office": "Офис", "Remote": "Удалённо", "Hybrid": "Гибрид"}.get(v.get("work_type", ""), v.get("work_type", ""))
    employment = {"Full-time": "Фултайм", "Part-time": "Парт-тайм", "Contract": "Контракт"}.get(v.get("type_of_employment", ""), v.get("type_of_employment", ""))
    levels = ", ".join(v.get("candidate_levels", []))
    if any([work_type, employment, levels]):
        meta = ", ".join(x for x in [work_type, employment, levels] if x)
        parts.append(f"**Формат:** {meta}")

    cities = ", ".join(v.get("cities", []))
    if cities:
        parts.append(f"**Города:** {cities}")

    tags = " ".join(v.get("tags", []))
    if tags:
        parts.append(f"**Стек:** {tags}")

    add("О компании", v.get("about_company", ""))
    add("Описание", v.get("description", ""))
    add("Обязанности", v.get("responsibilities", ""))
    add("Требования", v.get("requirements", ""))
    add("Условия", v.get("working_conditions", ""))
    add("Контакты", v.get("contacts", ""))

    return "\n\n".join(parts)


def get_job_url(msg: dict) -> str | None:
    text = msg.get("text", "")
    if not isinstance(text, list):
        return None
    for part in text:
        if isinstance(part, dict) and "ods.ai/jobs/" in part.get("href", ""):
            return part["href"]
    return None


def main():
    with open(INPUT, encoding="utf-8") as f:
        data = json.load(f)

    messages = data["messages"]
    print(f"Всего сообщений: {len(messages)}")

    enriched = 0
    failed = 0

    for i, msg in enumerate(messages):
        url = get_job_url(msg)
        if not url:
            continue

        print(f"[{i+1}/{len(messages)}] {url}")
        vacancy = fetch_vacancy(url)

        if vacancy is None:
            failed += 1
            continue

        full_text = vacancy_to_text(vacancy)

        # Заголовок (название вакансии + компания)
        title = vacancy.get("title", "")
        company = vacancy.get("company_name", "")
        header_parts = []
        if title:
            header_parts.append({"type": "bold", "text": title})
        if company:
            header_parts.append(f"\n{company}\n\n")

        # Собираем новый text: заголовок + полный текст + ссылка
        new_text = []
        if header_parts:
            new_text.extend(header_parts)
        new_text.append(full_text)
        new_text.append(f"\n\n[Ссылка на вакансию]({url})")

        msg["text"] = new_text

        # text_entities тоже обновляем — упрощаем до plain
        msg["text_entities"] = [{"type": "plain", "text": full_text}]

        enriched += 1
        time.sleep(0.3)  # не спамим сервер

    print(f"\nОбогащено: {enriched}, ошибок: {failed}")

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Сохранено в {OUTPUT}")


if __name__ == "__main__":
    main()
