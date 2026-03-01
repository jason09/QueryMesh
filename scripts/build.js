import fs from 'fs';
import path from 'path';

const root = path.resolve(process.cwd());
const srcDir = path.join(root, 'src');
const distDir = path.join(root, 'dist');

function rimraf(p) {
  if (!fs.existsSync(p)) return;
  for (const entry of fs.readdirSync(p)) {
    const full = path.join(p, entry);
    const st = fs.statSync(full);
    if (st.isDirectory()) rimraf(full);
    else fs.unlinkSync(full);
  }
  fs.rmdirSync(p);
}

function copyDir(from, to) {
  fs.mkdirSync(to, { recursive: true });
  for (const entry of fs.readdirSync(from)) {
    const src = path.join(from, entry);
    const dst = path.join(to, entry);
    const st = fs.statSync(src);
    if (st.isDirectory()) copyDir(src, dst);
    else fs.copyFileSync(src, dst);
  }
}

rimraf(distDir);
copyDir(srcDir, distDir);
console.log('Built to dist/.');
