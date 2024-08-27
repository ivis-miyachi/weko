from invenio_assets.webpack import WebpackThemeBundle

weko_search_ui = WebpackThemeBundle(
    __name__,
    "assets",
    default="bootstrap3",
    themes={
        "bootstrap3": dict(
            entry={
                'search-ui-css-weko-search-ui': './css/weko_search_ui/dot_leaders.css',
                'search-ui-js-js-dependencies-datepicker': './js/weko_search_ui/js_dependencies_datepicker.js',
                'search-ui-js-weko-search-ui': './js/weko_search_ui/app.js',
                'search-ui-js-import': './js/weko_search_ui/import.js',
                'search-ui-js-export': './js/weko_search_ui/export.js',
                #'search-ui-js-moment': './js/weko_search_ui/moment.min.js', TODO: htmlでこれを使用箇所をnode_modulesからの使用に変更
                'search-ui-less-import': './css/weko_search_ui/import.less',
                'search-ui-less-export': './css/weko_search_ui/export.less',
                'search-ui-js-facet-search': './js/weko_search_ui/facet_search_bundle.js',
                'search-ui-less-theme-facet-search': './css/weko_search_ui/facet_search_bundle.less'
            },
            dependencies={
                'angular': "~1.4.9",
                "bootstrap": "~3.3.7",
                "bootstrap-datepicker": "~1.7.1",
                "jquery": "~3.2.1",
                "react": "~15.6.1",
                "react-bootstrap": "~0.33.1",
                "react-dom": "~15.6.1",
                'moment':'~2.24'
            }
        )
    }
)
