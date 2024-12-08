"""
프로그램 이름: new_pol_03
작성자: 김민성
작성일: 2024-12-06
설명:
    - IEA(International Energy Agency) 웹사이트에서 정책 데이터를 스크래핑하는 프로그램입니다.
    - 정책 목록과 상세 정보를 병렬로 스크래핑하며, 결과를 CSV 파일로 저장합니다.
    - 주요 기능:
        1. 정책 목록 페이지 스크래핑 (정책명, URL, 국가, 연도 등 정보 수집)
        2. 개별 정책 상세 페이지 스크래핑 (내용, 주제, 정책 유형 등 세부 정보 수집)
        3. 정책 비용, 기간, 탄소 감소 비율 등 계산된 데이터 추가
        4. 데이터를 정리하여 CSV 파일로 저장
    - 주의사항:
        1. Python 환경에 필요한 라이브러리 설치 필요 (requests, BeautifulSoup, pandas 등)
        2. 서버 과부하를 방지하기 위해 병렬 처리 워커 수를 적절히 조정
"""
import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import numpy as np
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

# 콘솔 출력의 기본 인코딩을 UTF-8로 설정 (유니코드 처리를 위해 필요)
sys.stdout.reconfigure(encoding='utf-8')

# 기본 URL 설정
base_url = "https://www.iea.org"
policies_url = f"{base_url}/policies"

# 세션 객체 생성 (HTTP 연결 재사용 및 성능 향상)
session = requests.Session()

# HTML 콘텐츠를 파싱하기 위한 BeautifulSoup 객체 생성 함수
def get_soup(url):
    response = session.get(url)  # HTTP GET 요청 전송
    response.encoding = 'utf-8'  # 응답 인코딩 설정
    return BeautifulSoup(response.content, 'html.parser')

# 텍스트를 정리하고 불필요한 부분 제거
def clean_text(text):
    # 여러 공백을 하나로 줄이고, 특정 문자나 불필요한 텍스트를 제거
    text = re.sub(r'\s+', ' ', text)
    text = text.replace('\u200b', '')  # Zero-width space 제거
    text = text.replace('\u00a0', ' ')  # Non-breaking space를 일반 공백으로 변경
    text = text.replace('Want to know more about this policy ?', '')
    text = text.replace('Learn moreLearn more', '')
    text = text.replace('Remove Filter', '')
    return text.strip()

# 고유 값 생성: 정책 이름 및 URL 기반으로 계산된 값 생성
def generate_reproducible_value(base_str, min_val, max_val, decimal_places=0):
    # 문자열 해시를 생성하여 난수의 시드로 사용
    hash_value = int(hashlib.sha256(base_str.encode()).hexdigest(), 16)
    seed = hash_value % (2**32)  # 시드 값 범위 제한
    np.random.seed(seed)  # 난수 시드 설정
    value = np.random.uniform(min_val, max_val)  # 지정 범위 내에서 난수 생성
    return round(value, decimal_places)

# 정책 정보를 저장하는 클래스
class Policy:
    def __init__(self, name, url, country, year, status, jurisdiction):
        # 기본 속성 초기화
        self.name = name
        self.url = url
        self.country = country
        self.year = year
        self.status = status
        self.jurisdiction = jurisdiction
        self.last_updated = "N/A"  # 기본값 N/A로 설정
        self.content = "N/A"
        self.topics = "N/A"
        self.policy_types = "N/A"
        self.sectors = "N/A"
        self.implementation_cost = None
        self.duration = None
        self.estimated_reduction_percent = None
    
    # 개별 정책 상세 페이지를 스크래핑하는 함수
    def scrape_details(self):
        print(f"Scraping details for {self.url}...")
        soup = get_soup(self.url)

        # 'Last Updated' 정보 추출
        last_updated_element = None
        meta_elements = soup.select(".o-hero-freepage__meta")
        for element in meta_elements:
            if "Last updated" in element.text:
                last_updated_element = element.text.strip().replace("Last updated: ", "").strip()
                break
        self.last_updated = clean_text(last_updated_element) if last_updated_element else "N/A"

        # 정책 콘텐츠 추출
        content_paragraphs = soup.select(".m-block__content p")
        if content_paragraphs:
            content_text = clean_text(' '.join([p.text for p in content_paragraphs]))
            self.content = content_text if content_text else "N/A"
        else:
            self.content = "N/A"

        # 주제, 정책 유형, 섹터 정보 추출
        for section in soup.select(".m-policy-content__list"):
            title = section.select_one(".m-policy-content-list__title").text.strip()
            items = section.select(".m-policy-content-list__item a")
            items_text = ', '.join([clean_text(item.text.strip()) for item in items]) if items else "N/A"
            if "Topics" in title:
                self.topics = items_text
            elif "Policy types" in title:
                self.policy_types = items_text
            elif "Sectors" in title:
                self.sectors = items_text

        # 정책 관련 추가 값 생성 (비용, 기간, 감소율)
        self.generate_policy_values()

    # 정책 비용, 기간, 탄소 감소 비율을 생성
    def generate_policy_values(self):
        base_str = f"{self.name}_{self.url}"  # 정책 고유 문자열 생성
        self.implementation_cost = generate_reproducible_value(base_str, 1000000, 50000000, 0)
        self.duration = generate_reproducible_value(base_str, 3, 10, 0)
        self.estimated_reduction_percent = generate_reproducible_value(base_str, 1.0, 10.0, 2)

    # 객체 데이터를 딕셔너리로 변환
    def to_dict(self):
        return {
            "Policy": self.name,
            "Policy URL": self.url,
            "Country": self.country,
            "Year": self.year,
            "Status": self.status,
            "Jurisdiction": self.jurisdiction,
            "Last Updated": self.last_updated,
            "Content": self.content,
            "Topics": self.topics,
            "Policy Types": self.policy_types,
            "Sectors": self.sectors,
            "Implementation Cost": self.implementation_cost,
            "Duration": self.duration,
            "Estimated Carbon Reduction (%)": self.estimated_reduction_percent,
        }

# 특정 페이지의 정책 목록을 스크래핑
def scrape_policies_page(page_number):
    url = f"{policies_url}?page={page_number}"
    print(f"Scraping page {page_number}...")
    soup = get_soup(url)
    policies = []
    for item in soup.select(".m-policy-listing-item-row__content"):
        name = clean_text(item.select_one(".m-policy-listing-item__link").text.strip())
        policy_url = base_url + item.select_one(".m-policy-listing-item__link")['href']
        country = clean_text(item.select_one(".m-policy-listing-item__col--country").text.strip())
        year = clean_text(item.select_one(".m-policy-listing-item__col--year").text.strip())
        status = clean_text(item.select_one(".m-policy-listing-item__col--status").text.strip())
        jurisdiction = clean_text(item.select_one(".m-policy-listing-item__col--jurisdiction").text.strip())
        
        # Policy 객체 생성 및 목록에 추가
        policy = Policy(name, policy_url, country, year, status, jurisdiction)
        policies.append(policy)
    print(f"Found {len(policies)} policies on page {page_number}.")
    return policies

# 메인 함수
def main():
    print("Starting the scraping process...")
    start_time = time.time()

    all_policies = []  # 모든 정책 데이터를 저장할 리스트
    total_pages = 441  # 스크래핑할 총 페이지 수

    # 정책 목록을 병렬 처리로 스크래핑
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(scrape_policies_page, page) for page in range(1, total_pages + 1)]
        for future in as_completed(futures):
            all_policies.extend(future.result())

    print(f"Found a total of {len(all_policies)} policies.")
    
    # 정책 상세 정보를 병렬 처리로 스크래핑
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(policy.scrape_details): policy for policy in all_policies}
        for future in as_completed(futures):
            future.result()

    # 데이터프레임 생성
    policies_dicts = [policy.to_dict() for policy in all_policies]
    df = pd.DataFrame(policies_dicts)
    
    # 데이터 정리 및 저장
    df['Last Updated'] = df['Last Updated'].replace('Last updated:', 'N/A')
    df.fillna('N/A', inplace=True)
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    df = df.sort_values(by='Year', ascending=False)
    df.to_csv('new_policy_001.csv', index=False, encoding='utf-8-sig')
    print("Scraping completed and saved to policies_parallel_clean_updated_sorted001.csv")

    end_time = time.time()
    print(f"Total time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
