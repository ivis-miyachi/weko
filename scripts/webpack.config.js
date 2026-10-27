/*
 * This file is part of Invenio.
 * Copyright (C) 2017-2018 CERN.
 * Copyright (C) 2022 Graz University of Technology.
 *
 * Invenio is free software; you can redistribute it and/or modify it
 * under the terms of the MIT License; see LICENSE file for more details.
 */

const fs = require("fs");
const path = require("path");
const { CleanWebpackPlugin } = require("clean-webpack-plugin");
const config = require("./config");
const BundleTracker = require("webpack-bundle-tracker");
const MiniCssExtractPlugin = require("mini-css-extract-plugin");
const OptimizeCSSAssetsPlugin = require("optimize-css-assets-webpack-plugin");
const safePostCssParser = require("postcss-safe-parser");
const TerserPlugin = require("terser-webpack-plugin");
const webpack = require("webpack");
const ESLintPlugin = require("eslint-webpack-plugin");
const { ESBuildMinifyPlugin } = require("esbuild-loader");

// Load aliases from config and resolve their full path
let aliases = {};
if (config.aliases) {
  aliases = Object.fromEntries(
    Object.entries(config.aliases).map(([alias, alias_path]) => [
      alias,
      path.resolve(config.build.context, alias_path),
    ])
  );
}

// Ensure Select2 compatibility modules are available for legacy integrations.
// Map bare "select2" imports to the full build instead of core build.
aliases["select2$"] = path.resolve(
  config.build.context,
  "./node_modules/select2/dist/js/select2.full.js"
);

var webpackConfig = {
  mode: process.env.NODE_ENV,
  entry: config.entry,
  context: config.build.context,
  externals: [
    {
      react: 'React',
      'react-dom': 'ReactDOM',
    }
  ],
  resolve: {
    extensions: ["*", ".js", ".jsx"],
    symlinks: false,
    alias: aliases,
  },
  output: {
    path: config.build.assetsPath,
    filename: "js/[name].[chunkhash].js",
    chunkFilename: "js/[id].[chunkhash].js",
    publicPath: config.build.assetsURL,
    jsonpFunction: "wekoWebpackJsonp",
  },
  optimization: {
    minimizer: [
      new ESBuildMinifyPlugin({
        target: "es2015",                         // "es5" は既存 chunk 中の const でエラーになるため不可
        supported: { "template-literal": false, "object-extensions": false }, // テンプレートリテラル化させない / IE11 等 ES5 環境向けにオブジェクト短縮記法を出力しない
        minifyIdentifiers: false,                 // 旧設定 mangle:false を踏襲 (AngularJS 暗黙 DI 保護)
        minifySyntax: true,
        minifyWhitespace: true,
        legalComments: "eof",                     // ライセンスコメントはファイル末尾に保持
        css: true,                                // CSS も esbuild で圧縮 (cssnano を置換)
      }),
    ],
    splitChunks: {
      chunks: "all",
    },
    // Extract webpack runtime and module manifest to its own file in order to
    // prevent vendor hash from being updated whenever app bundle is updated.
    runtimeChunk: {
      name: "manifest",
    },
  },
  module: {
    rules: [
      {
        test: require.resolve("jquery"),
        use: [
          {
            loader: "expose-loader",
            options: {
              exposes: ["$", "jQuery"],
            },
          },
        ],
      },
      {
        test: /\.(js|jsx)$/,
        exclude: [/node_modules/, /@babel(?:\/|\\{1,2})runtime/],
        use: [
          {
            loader: "babel-loader",
            options: {
              presets: ["@babel/preset-env", "@babel/preset-react"],
              plugins: [
                "@babel/plugin-proposal-class-properties",
                "@babel/plugin-transform-runtime",
              ],
              cacheDirectory: true,
            },
          },
        ],
      },
      {
        test: /\.(scss|css)$/,
        use: [MiniCssExtractPlugin.loader, "css-loader", "sass-loader"],
      },
      {
        test: /\.(less)$/,
        use: [MiniCssExtractPlugin.loader, "css-loader", "less-loader"],
      },
      // Inline images smaller than 10k
      {
        test: /\.(png|cur|jpe?g|gif|svg)(\?.*)?$/,
        use: [
          {
            loader: require.resolve("url-loader"),
            options: {
              limit: 10000,
              name: "img/[name].[hash:7].[ext]",
            },
          },
        ],
      },
      // Inline webfonts smaller than 10k
      {
        test: /\.(woff2?|eot|ttf|otf)(\?.*)?$/,
        use: [
          {
            loader: require.resolve("file-loader"),
            options: {
              limit: 10000,
              name: "fonts/[name].[hash:7].[ext]",
            },
          },
        ],
      },
    ],
  },
  devtool:
    process.env.NODE_ENV === "production" ? "none" : "inline-source-map",
  plugins: [
    new ESLintPlugin({emitWarning: true,
              quiet: true,
              formatter: require("eslint-friendly-formatter"),
              eslintPath: require.resolve("eslint")}
    ),
    // Pragmas
    new webpack.DefinePlugin({
      "process.env": '"production"',
    }),
    new MiniCssExtractPlugin({
      // Options similar to the same options in webpackOptions.output
      // both options are optional
      filename: "css/[name].[contenthash].css",
      chunkFilename: "css/[name].[contenthash].css",
    }),
    // Removes the dist folder before each run.
    new CleanWebpackPlugin({
      dry: false,
      verbose: false,
      dangerouslyAllowCleanPatternsOutsideProject: true,
    }),
    // Automatically inject jquery
    new webpack.ProvidePlugin({
      jQuery: "jquery",
      $: "jquery",
      jquery: "jquery",
      "window.jQuery": "jquery",
    }),
    // Write manifest file which Python will read.
    new BundleTracker({
      path: config.build.assetsPath,
      filename: path.join(config.build.assetsPath, "manifest.json") ,
      publicPath: config.build.assetsURL,
    }),
  ],
  performance: { hints: false },
};

if (process.env.npm_config_report) {
  var BundleAnalyzerPlugin = require("webpack-bundle-analyzer")
    .BundleAnalyzerPlugin;
  webpackConfig.plugins.push(new BundleAnalyzerPlugin());
}

if (process.env.NODE_ENV === "development") {
  const LiveReloadPlugin = require("webpack-livereload-plugin");
  webpackConfig.plugins.push(new LiveReloadPlugin());
}

module.exports = webpackConfig;
