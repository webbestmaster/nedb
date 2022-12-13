// eslint-disable-next-line unicorn/prefer-module
module.exports = {
    moduleNameMapper: {
        '^\\S+.(css|styl|less|sass|scss|png|jpg|ttf|woff|woff2)$': 'jest-transform-stub',
    },
    modulePathIgnorePatterns: ['<rootDir>/tsc-check/'],
    preset: 'ts-jest',
    rootDir: '../../',
    setupFilesAfterEnv: [],
    testEnvironment: 'node',
    // maxConcurrency: 1,
    // maxWorkers: 1,
    testTimeout: 10e3,
};
