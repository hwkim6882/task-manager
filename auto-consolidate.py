#!/usr/bin/env python3
"""
할 일 자동 통합 스크립트
- 미완료 태스크가 30개 초과시 Claude AI로 유사 항목 병합
- 6시간마다 LaunchAgent로 자동 실행
"""
import os
import sys
import json
import time
import logging
import re
from pathlib import Path
from datetime import datetime
from copy import deepcopy

import firebase_admin
from firebase_admin import credentials, firestore
import anthropic

# ===== 설정 =====
BASE_DIR = Path(__file__).parent
FIREBASE_KEY = str(BASE_DIR / "task-manager-4759b-firebase-adminsdk-fbsvc-3052457cde.json")
USER_UID = "B06hNRMchKbbN9YaT5yU4x5jin82"
PENDING_THRESHOLD = 30
TARGET_RANGE = "20~35"
BACKUP_DIR = "/tmp"

# 로깅
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE_DIR / 'consolidate.log'), encoding='utf-8')
    ]
)
log = logging.getLogger(__name__)


def init_claude():
    key_file = BASE_DIR / '.claude_api_key'
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key and key_file.exists():
        api_key = key_file.read_text().strip()
    if not api_key:
        log.error("API 키 없음")
        sys.exit(1)
    return anthropic.Anthropic(api_key=api_key)


def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def read_tasks(db):
    doc = db.collection('users').document(USER_UID).get()
    if not doc.exists:
        return []
    return doc.to_dict().get('tasks', [])


def write_tasks(db, tasks):
    db.collection('users').document(USER_UID).update({
        'tasks': tasks,
        'updatedAt': firestore.SERVER_TIMESTAMP
    })


def save_backup(tasks):
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    path = os.path.join(BACKUP_DIR, f'tasks_backup_{ts}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"백업: {path} ({len(tasks)}건)")
    return path


def consolidate_with_ai(client, pending):
    task_list = '\n'.join([
        f'- #{i} "{t.get("title","?")}" [{t.get("category","?")}] ({t.get("timeMin",0)}분)'
        for i, t in enumerate(pending)
    ])

    prompt = f"""당신은 법률사무소의 업무 관리 전문가입니다.
아래 {len(pending)}건의 미완료 업무를 사건/인물/주제별로 통합하여 {TARGET_RANGE}건으로 줄여주세요.

중요 규칙:
1. 같은 사건/인물/회사 관련 업무는 반드시 1건으로 합침
2. 같은 성격의 소소한 업무끼리 묶음
3. "기타" 그룹은 만들지 마세요. 모든 업무를 구체적 그룹에 배정하세요.
4. 모든 {len(pending)}건을 빠짐없이 배정하세요.

★ 가장 중요: title은 통합된 모든 세부 업무의 핵심 키워드를 포함해야 합니다.
예시: "천현철 형사공판 대응 (계약체결, 미수금정산, 협상전략, 수사기록열람, 증인확보, 배임법리검토)"
이렇게 괄호 안에 세부 키워드를 나열하여 기존 항목들을 한눈에 파악할 수 있게 해주세요.

JSON 배열로 반환:
[
  {{ "title": "통합 제목 (세부키워드1, 세부키워드2, ...)", "indices": [#번호들], "total_time": 합산시간, "category": "카테고리" }}
]

JSON 배열만 반환하세요.

업무 목록:
{task_list}"""

    try:
        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=16384,
            messages=[{"role": "user", "content": prompt}]
        )
        text = resp.content[0].text
        match = re.search(r'```(?:json)?\s*([\s\S]*?)```', text)
        if match:
            text = match.group(1)
        groups = json.loads(text.strip())
        log.info(f"AI 결과: {len(groups)}개 그룹")
        return groups
    except Exception as e:
        log.error(f"AI 통합 실패: {e}")
        return None


def apply_consolidation(pending, done_tasks, groups):
    merged = []
    used = set()

    for g in groups:
        indices = g.get('indices', [])
        valid = [i for i in indices if 0 <= i < len(pending)]
        if not valid:
            continue
        used.update(valid)

        base = deepcopy(pending[valid[0]])
        base['title'] = g.get('title', base['title'])
        base['category'] = g.get('category', base.get('category', '로펌업무'))
        base['timeMin'] = g.get('total_time', sum(pending[i].get('timeMin', 0) for i in valid))
        base['impact'] = min(5, max(1, len(valid) // 3 + 1))
        base['id'] = time.time() * 1000 + len(merged)
        base['mergedFrom'] = [pending[i].get('title', '') for i in valid]
        base['mergedAt'] = datetime.now().isoformat()
        merged.append(base)

    # orphan 복구
    for i in range(len(pending)):
        if i not in used:
            log.warning(f"누락 복구: #{i} {pending[i].get('title','')}")
            merged.append(pending[i])

    return done_tasks + merged, len(merged)


def main():
    dry_run = '--dry-run' in sys.argv
    force = '--force' in sys.argv
    threshold = 0 if force else PENDING_THRESHOLD

    log.info("=" * 50)
    log.info(f"할 일 자동 통합 시작 ({datetime.now().isoformat()})")
    if dry_run:
        log.info("[DRY RUN 모드]")
    log.info("=" * 50)

    db = init_firestore()
    claude = init_claude()

    all_tasks = read_tasks(db)
    pending = [t for t in all_tasks if not t.get('done', False)]
    done = [t for t in all_tasks if t.get('done', False)]

    log.info(f"전체: {len(all_tasks)}건 (미완료: {len(pending)}, 완료: {len(done)})")

    if len(pending) <= threshold:
        log.info(f"미완료 {len(pending)}건 <= 임계값 {threshold}건. 통합 불필요.")
        return

    log.info(f"미완료 {len(pending)}건 > 임계값 {threshold}건 → 통합 실행")

    backup = save_backup(all_tasks)

    groups = consolidate_with_ai(claude, pending)
    if not groups:
        log.error("AI 결과 없음. 종료.")
        return

    assigned = sum(len(g.get('indices', [])) for g in groups)
    log.info(f"배정: {assigned}/{len(pending)}")

    new_tasks, merged_count = apply_consolidation(pending, done, groups)

    log.info(f"통합: {len(pending)}건 → {merged_count}건")

    if dry_run:
        log.info("[DRY RUN] Firestore 미적용")
        for g in groups:
            if len(g.get('indices', [])) > 1:
                log.info(f"  {g['title']} ← {len(g['indices'])}건")
    else:
        write_tasks(db, new_tasks)
        log.info("Firestore 업데이트 완료")

    log.info(f"백업: {backup}")
    log.info("완료!")


if __name__ == '__main__':
    main()
