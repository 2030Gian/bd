import struct
from backend.core.record import Record
from backend.catalog.catalog import get_json
from backend.core.utils import build_format

BUCKET_SIZE = 3
HEADER_FORMAT = 'ii'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
INITIAL_MAX_CHAIN = 2


# ================== Bucket ==================
class Bucket:
    def __init__(self, local_depth=1, overflow_page=-1):
        self.records = []
        self.local_depth = local_depth
        self.overflow_page = overflow_page

    def is_full(self):
        return len(self.records) >= BUCKET_SIZE

    def is_empty(self):
        return len(self.records) == 0

    def put(self, record):
        if not self.is_full():
            self.records.append(record)
            return True
        return False

    def find(self, key_value, key_name):
        return [rec for rec in self.records if rec.fields[key_name] == key_value]

    def remove(self, key_value, key_name):
        removed = []
        i = 0
        while i < len(self.records):
            if self.records[i].fields[key_name] == key_value:
                removed.append(self.records.pop(i))
            else:
                i += 1
        return removed if removed else None

    def pack(self, record_size, record_format, schema):
        packed = b''.join(rec.pack() for rec in self.records[:BUCKET_SIZE])
        padding = b'\x00' * (BUCKET_SIZE * record_size - len(packed))
        return packed + padding

    @classmethod
    def unpack(cls, data, local_depth, overflow_page, record_size, record_format, schema):
        bucket = cls(local_depth, overflow_page)
        for i in range(BUCKET_SIZE):
            off = i * record_size
            chunk = data[off: off + record_size]
            if chunk == b'\x00' * record_size:
                continue
            try:
                record = Record.unpack(chunk, record_format, schema)
                if hasattr(record, 'fields') and isinstance(record.fields, dict):
                    if not record.fields.get("deleted", False):
                        bucket.records.append(record)
            except Exception:
                continue
        return bucket


# ================== Hash Extensible ==================
class ExtendibleHashingFile:

    def __init__(self, filename: str):
        self.filename = filename
        self.schema = get_json(self.filename)[0]
        self.format = build_format(self.schema)
        self.record_size = struct.calcsize(self.format)
        self.bucket_disk_size = self.record_size * BUCKET_SIZE

        # Estado en memoria
        self.global_depth = 1
        self.directory = [0, 1]
        self.next_page_idx = 2
        self.read_count = 0
        self.write_count = 0

        self.key_name = None
        self._load_or_init()

    def _json_offset(self) -> int:
        try:
            with open(self.filename, 'rb') as f:
                b = f.read(4)
                if not b or len(b) < 4:
                    return 0
                size = struct.unpack('I', b)[0]
                return 4 + size
        except FileNotFoundError:
            return 0

    def _pages_base_offset(self) -> int:
        return self._json_offset() + 8 + len(self.directory) * 4

    def _get_page_offset(self, page_idx):
        return self._pages_base_offset() + page_idx * (HEADER_SIZE + self.bucket_disk_size)

    def _hash(self, key):
        if isinstance(key, str):
            return sum(ord(c) for c in key)
        return int(key)

    def _get_bucket_idx(self, key):
        h = self._hash(key)
        return h & ((1 << self.global_depth) - 1)

    def _get_key_hash_prefix(self, key, depth):
        h = self._hash(key)
        return h & ((1 << depth) - 1)

    def _max_chain_length(self):
        return INITIAL_MAX_CHAIN + self.global_depth

    def _load_or_init(self):
        try:
            with open(self.filename, 'r+b') as f:
                off = self._json_offset()
                f.seek(off)
                header = f.read(8)
                if not header or header == b'\x00' * 8:
                    self._init_file()
                    return
                self.read_count += 1
                self.global_depth, self.next_page_idx = struct.unpack('ii', header)
                dir_size = 1 << self.global_depth
                dir_bytes = f.read(dir_size * 4)
                if len(dir_bytes) != dir_size * 4:
                    self._init_file()
                    return
                self.directory = list(struct.unpack(f'{dir_size}i', dir_bytes))
        except FileNotFoundError:
            self._init_file()

    def _init_file(self):
        self.global_depth = 1
        self.directory = [0, 1]
        self.next_page_idx = 2
        try:
            f = open(self.filename, 'r+b')
        except FileNotFoundError:
            f = open(self.filename, 'w+b')
        with f:
            off = self._json_offset()
            f.seek(off)
            f.write(struct.pack('ii', self.global_depth, self.next_page_idx))
            f.write(struct.pack(f'{len(self.directory)}i', *self.directory))

            f.seek(self._get_page_offset(0))
            f.write(struct.pack('i', 1))
            f.write(struct.pack('i', -1))
            f.write(b'\x00' * self.bucket_disk_size)

            f.seek(self._get_page_offset(1))
            f.write(struct.pack('i', 1))
            f.write(struct.pack('i', -1))
            f.write(b'\x00' * self.bucket_disk_size)

            self.write_count += 3

    def _read_bucket(self, page_idx):
        with open(self.filename, 'rb') as f:
            offset = self._get_page_offset(page_idx)
            f.seek(offset)
            local_depth = struct.unpack('i', f.read(4))[0]
            overflow_page = struct.unpack('i', f.read(4))[0]
            data = f.read(self.bucket_disk_size)
            self.read_count += 1
            return Bucket.unpack(data, local_depth, overflow_page,
                                 self.record_size, self.format, self.schema)

    def _write_bucket(self, page_idx, bucket):
        with open(self.filename, 'r+b') as f:
            offset = self._get_page_offset(page_idx)
            f.seek(offset)
            f.write(struct.pack('i', bucket.local_depth))
            f.write(struct.pack('i', bucket.overflow_page))
            f.write(bucket.pack(self.record_size, self.format, self.schema))
            self.write_count += 1

    def _write_directory(self):
        with open(self.filename, 'r+b') as f:
            base = self._json_offset()
            f.seek(base)
            f.write(struct.pack('ii', self.global_depth, self.next_page_idx))
            f.write(struct.pack(f'{len(self.directory)}i', *self.directory))
            self.write_count += 1

    def _read_chain(self, page_idx):
        chain = []
        cur = page_idx
        while cur != -1:
            b = self._read_bucket(cur)
            chain.append((cur, b))
            cur = b.overflow_page
        return chain

    def _write_chain(self, chain):
        for page_idx, bucket in chain:
            self._write_bucket(page_idx, bucket)

    def find(self, key_value, key_name="id"):
        if self.key_name is None:
            self.key_name = key_name
        dir_idx = self._get_bucket_idx(key_value)
        page_idx = self.directory[dir_idx]
        result = []
        for _, bucket in self._read_chain(page_idx):
            for rec in bucket.records:
                if rec.fields[key_name] == key_value and not rec.fields.get("deleted", False):
                    result.append({k: v for k, v in rec.fields.items() if k != 'deleted'})
        return result

    def insert(self, record_data, key_name="id"):
        if self.key_name is None:
            self.key_name = key_name

        key_value = record_data[key_name]
        rec = Record(self.schema, self.format, record_data)

        # Usamos un bucle para manejar el caso en que un split nos obligue a reintentar la inserción
        while True:
            dir_idx = self._get_bucket_idx(key_value)
            page_idx = self.directory[dir_idx]
            chain = self._read_chain(page_idx)

            # 1) Intentar insertar en algún bucket de la cadena
            for curr_page, curr_bucket in chain:
                if not curr_bucket.is_full():
                    curr_bucket.put(rec)
                    self._write_bucket(curr_page, curr_bucket)
                    return record_data

            # 2) Si la cadena aún no alcanzó el máximo -> encadenar otro bucket
            if len(chain) < self._max_chain_length():
                last_page, last_bucket = chain[-1]
                new_overflow_page = self.next_page_idx
                self.next_page_idx += 1

                last_bucket.overflow_page = new_overflow_page
                self._write_bucket(last_page, last_bucket)

                new_overflow = Bucket(local_depth=last_bucket.local_depth)
                new_overflow.put(rec)
                self._write_bucket(new_overflow_page, new_overflow)
                self._write_directory()  # Actualiza next_page_idx
                return record_data

            # 3) Si se alcanzó el máximo de la cadena -> hacer split y reintentar
            self._split(page_idx)
            # El bucle while se encargará de reintentar la inserción

    def _split(self, page_idx):
        chain = self._read_chain(page_idx)
        head_page, head_bucket = chain[0]
        old_local_depth = head_bucket.local_depth

        # Si la profundidad local es igual a la global, duplicamos el directorio
        if old_local_depth == self.global_depth:
            self.directory.extend(self.directory)
            self.global_depth += 1

        new_page_idx = self.next_page_idx
        self.next_page_idx += 1

        new_bucket = Bucket(local_depth=old_local_depth + 1)
        head_bucket.local_depth = old_local_depth + 1

        # Redistribuir punteros del directorio
        bit_to_check = 1 << old_local_depth
        for i in range(len(self.directory)):
            if self.directory[i] == page_idx and (i & bit_to_check):
                self.directory[i] = new_page_idx

        self._write_directory()

        # Redistribuir registros
        all_records = [record for _, b in chain for record in b.records]

        # Limpiar la cadena original
        head_bucket.records = []
        head_bucket.overflow_page = -1
        # Liberar páginas de desborde (en una implementación más compleja se añadirían a una lista de libres)
        for p_idx, b in chain[1:]:
            b.records = []
            self._write_bucket(p_idx, b)  # Opcional: marcar como libre

        new_chain = [(head_page, head_bucket)]
        new_chain_overflow = [(new_page_idx, new_bucket)]

        self._write_bucket(head_page, head_bucket)
        self._write_bucket(new_page_idx, new_bucket)

        key_name = self.key_name or "id"
        for record in all_records:
            key_value = record.fields[key_name]
            h_prefix = self._get_key_hash_prefix(key_value, old_local_depth + 1)

            target_chain = new_chain if (h_prefix & bit_to_check) == 0 else new_chain_overflow

            inserted = False
            for p, b in target_chain:
                if not b.is_full():
                    b.put(record)
                    self._write_bucket(p, b)
                    inserted = True
                    break

            if not inserted:  # Añadir página de desborde si es necesario
                last_p, last_b = target_chain[-1]
                new_p = self.next_page_idx
                self.next_page_idx += 1

                last_b.overflow_page = new_p
                self._write_bucket(last_p, last_b)

                new_b = Bucket(local_depth=old_local_depth + 1)
                new_b.put(record)
                self._write_bucket(new_p, new_b)
                target_chain.append((new_p, new_b))
                self._write_directory()

    def remove(self, key_value, key_name="id"):
        if self.key_name is None:
            self.key_name = key_name
        dir_idx = self._get_bucket_idx(key_value)
        page_idx = self.directory[dir_idx]
        chain = self._read_chain(page_idx)

        removed_records = []
        for curr_page, curr_bucket in chain:
            removed = curr_bucket.remove(key_value, key_name)
            if removed:
                self._write_bucket(curr_page, curr_bucket)
                removed_records.extend([r.fields for r in removed])

        if not removed_records:
            return None

        # Intenta fusionar si el bucket principal está vacío
        # (implementación simplificada: solo fusiona si el bucket está completamente vacío)
        head_page, head_bucket = chain[0]
        if head_bucket.is_empty() and head_bucket.local_depth > 1:
            self._try_merge(head_page)

        return removed_records

    def _try_merge(self, page_idx):
        bucket = self._read_bucket(page_idx)
        if not bucket.is_empty() or bucket.local_depth <= 1:
            return

        # Encontrar el "buddy" bucket
        buddy_prefix_mask = (1 << bucket.local_depth) - 1
        my_prefix = self._get_key_hash_prefix(bucket.records[0].fields[self.key_name],
                                              bucket.local_depth) if bucket.records else 0  # Simplificación

        # El buddy tiene el bit más significativo invertido
        buddy_bit = 1 << (bucket.local_depth - 1)
        buddy_prefix = my_prefix ^ buddy_bit

        buddy_page_idx = -1
        for i, p_idx in enumerate(self.directory):
            if self._get_key_hash_prefix(i, bucket.local_depth) == buddy_prefix:
                buddy_page_idx = p_idx
                break

        if buddy_page_idx == -1 or buddy_page_idx == page_idx:
            return

        buddy_bucket = self._read_bucket(buddy_page_idx)

        # Condición de fusión: misma profundidad local y suma de registros cabe en un bucket
        if buddy_bucket.local_depth == bucket.local_depth and (
                len(bucket.records) + len(buddy_bucket.records)) <= BUCKET_SIZE:
            # Mover registros al buddy
            buddy_bucket.records.extend(bucket.records)
            buddy_bucket.local_depth -= 1

            # Actualizar directorio
            for i in range(len(self.directory)):
                if self.directory[i] == page_idx:
                    self.directory[i] = buddy_page_idx

            self._write_bucket(buddy_page_idx, buddy_bucket)
            # La página page_idx ahora está libre y podría ser reutilizada

            # Intentar reducir el directorio si es posible
            can_shrink = True
            if self.global_depth > 1:
                mid = len(self.directory) // 2
                for i in range(mid):
                    if self.directory[i] != self.directory[i + mid]:
                        can_shrink = False
                        break
                if can_shrink:
                    self.global_depth -= 1
                    self.directory = self.directory[:mid]

            self._write_directory()

    def get_all_records(self):
        all_records = []
        seen_pages = set()
        for page_idx in self.directory:
            if page_idx in seen_pages:
                continue
            chain = self._read_chain(page_idx)
            for curr_page, curr_bucket in chain:
                if curr_page in seen_pages:
                    continue
                seen_pages.add(curr_page)
                for record in curr_bucket.records:
                    if not record.fields.get("deleted", False):
                        all_records.append(record.fields)
        return all_records