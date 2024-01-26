import { compareHashed, hash, createJWT, decodeJWT, verifyJWT } from './index.js';

const passwords = ['aaa', 'bbb', 'ABaaa', 33333, 'aAd1333aa#_=', 'A ass _33A$..', 'aássss'];

(async () => {
    passwords.forEach(async p => {
        const hashed = await hash(p);
        const res = await compareHashed(p.toString(), hashed);
        console.log(p, hashed, res);
    });
})()

