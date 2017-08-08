var gulp = require('gulp');
var clean = require('gulp-clean');
var zip = require('gulp-zip');
var shell = require('gulp-shell');
var gulpSequence = require('gulp-sequence')
var package = require('./package.json');

var archive_name = 'AWSLambdaRedshiftLoader-' + package.version + '.zip';
var temp_dir = 'dist/.temp' + (new Date().valueOf());

gulp.task('zip:delete-archive', function () {
    return gulp.src('dist/' + archive_name, {read: false})
        .pipe(clean());
});

gulp.task('zip:copy-content', function () {
	return gulp.src(['index.js', 
	'common.js',
	'createS3TriggerFile.js',
	'constants.js',
	'kmsCrypto.js',
	'upgrades.js',
	'*.txt',
	'package.json', 
	'node_modules/**'], {base: '.'})
        .pipe(gulp.dest(temp_dir));
});

gulp.task('zip:prune-content', function () {
    return gulp.src(temp_dir, {read: false})
        .pipe(shell('npm prune --production', {cwd: temp_dir}));
});

gulp.task('zip:pack-lambda', function () {
    return gulp.src(temp_dir + '/**')
        .pipe(zip(archive_name))
        .pipe(gulp.dest('dist'));
});

gulp.task('zip:delete-temp', function () {
    return gulp.src(temp_dir, {read: false})
        .pipe(clean());
});

gulp.task('zip', gulpSequence(['zip:delete-archive', 'zip:copy-content'], 'zip:prune-content', 'zip:pack-lambda', 'zip:delete-temp'));
