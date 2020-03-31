/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

KylinApp.factory('TableService', ['$resource', function ($resource, config) {
  return $resource(Config.service.url + 'tables/:pro/:tableName/:action/:database', {}, {
    list: {method: 'GET', params: {}, cache: true, isArray: true},
    get: {method: 'GET', params: {}, isArray: false},
    loadHiveTable: {method: 'POST', params: {}, isArray: false},
    unLoadHiveTable: {method: 'DELETE', params: {}, isArray: false},
    genCardinality: {method: 'PUT', params: {action: 'cardinality'}, isArray: false},
    showHiveDatabases: {method: 'GET', params: {action:'hive'}, cache: true, isArray: true},
    showHiveTables: {method: 'GET', params: {action:'hive'}, cache: true, isArray: true},
    getSnapshots: {method: 'GET', params: {action: 'snapshots'}, isArray: true},
    getSupportedDatetimePatterns: {method: 'GET', params: {action: 'supported_datetime_patterns'}, isArray: true}
  });
}]);

KylinApp.service('CsvUploadService', function($http, $q) {
  this.upload = function(file, has_header, separator) {
    var deferred = $q.defer();
    var formData = new FormData();
    formData.append('file', file);
    formData.append('withHeader', has_header);
    formData.append('separator', separator);

    $http.post(Config.service.url + 'tables/fetchCsvData', formData, {
      transformRequest: angular.identity,
      transformResponse: angular.identity,
      headers: {
        'Content-Type': undefined
      }
    })
      .then(
        function (response) {
          deferred.resolve(response.data);
        },
        function (errResponse) {
          deferred.reject(errResponse);
        }
      );
    return deferred.promise;
  };

  this.save = function(file, table_name, project, columns, separator) {
    var deferred = $q.defer();
    var formData = new FormData();
    formData.append('file', file);
    formData.append('tableName', table_name);
    formData.append('project', project);
    formData.append('columns', columns);
    formData.append('separator', separator);

    $http.post(Config.service.url + 'tables/saveCsvTable', formData, {
      transformRequest: angular.identity,
      transformResponse: angular.identity,
      headers: {
        'Content-Type': undefined
      }
    })
      .then(
        function (response) {
          deferred.resolve(response.data);
        },
        function (errResponse) {
          deferred.reject(errResponse);
        }
      );
    return deferred.promise;
  }
});
