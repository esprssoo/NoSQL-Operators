import { Component, OnInit } from '@angular/core';

import { QuoteService } from './quote.service';
import { themeFromMapBox } from '../shell/shell.service';
import * as L from 'leaflet';
import { Options } from 'ng5-slider';
import * as _ from 'lodash';

import 'leaflet/dist/images/marker-icon.png';
import 'leaflet/dist/images/marker-icon-2x.png';
import 'leaflet/dist/images/marker-shadow.png';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss'],
})
export class HomeComponent implements OnInit {
  dataFromServer: string;
  data: Array<any> = [];
  groupedData: any;
  idArray: Array<any> = [];
  isSpatial = false;

  changeFloor: boolean;
  changeCeil: boolean;
  fps: number;
  addTime: number;
  windowBetweenFloorAndCeil: number;

  activeArray: Array<any> = [];

  value: number = 5;
  maxValue: number = 8;

  opt: Options = {
    floor: 0,
    ceil: 10,
  };
  map: L.Map;
  layers: Array<any> = [];
  options = {
    layers: [L.tileLayer(themeFromMapBox, { maxZoom: 18, attribution: '...' })],
    zoom: 4,
    center: L.latLng(33.88889, -118.48143),
  };

  constructor(
    private quoteService: QuoteService,
    private activeRoute: ActivatedRoute
  ) {}

  ngOnInit() {
    this.fps = 0.2;
    this.windowBetweenFloorAndCeil = 3;
    this.changeFloor = false;
    this.changeCeil = false;

    this.activeRoute.paramMap.subscribe((params: any) => {
      const caseOfView = params.get('case');
      if (caseOfView === 'spatial') {
        this.isSpatial = true;
      }
      if (caseOfView === 'spatiotemporal') {
        this.isSpatial = false;
      }
    });

    // Take data from serve from quoteService
    this.dataFromServer = this.quoteService.getData();

    console.log(
      'edwwwwwwwwwwwwwwwwwwwedwwwwwwwwwwwwwwwwwwwedwwwwwwwwwwwwwwwwwwwedwwwwwwwwwwwwwwwwwwwedwwwwwwwwwwwwwwwwwww',
      this.dataFromServer
    );

    // Responce for noda server connection
    this.quoteService.getConnectionMessage().then((res: any) => {
      console.log(res);
    });

    if (this.isSpatial === false) {
      let parsedData = JSON.parse(this.dataFromServer);
      this.data = parsedData['data'];

      // JS Date needs milli Epoch Timestamp (so below is a milli epoch converter)
      console.log(
        this.data,
        'ayto einai ena date: ' +
          new Date(parseInt(this.timestampManipulation(this.data[0]['time'])))
      );

      this.opt = {
        floor: parseInt(this.timestampManipulation(this.data[0]['time'])),
        ceil: parseInt(
          this.timestampManipulation(this.data[this.data.length - 1]['time'])
        ),
      };

      console.log('floor', this.opt.floor, 'ceil', this.opt.ceil);

      this.value = this.opt.floor;
      this.maxValue =
        this.opt.floor + this.windowBetweenFloorAndCeil * 60 * 60 * 1000;

      this.groupedData = _.groupBy(this.data, 'time');
      console.log('auta einai ta grouparismena data', this.groupedData);

      // for (let key in this.groupedData) {
      //   this.opt.ticksArray.push(parseInt(this.timestampManipulation(key)));
      // }

      // console.log('ticksArray: ', this.opt.ticksArray);

      let o = Math.round;
      let r = Math.random;
      let s = 255;
      this.data.forEach((element) => {
        let index = this.idArray.findIndex((id) => id.id === element.id);

        if (index === -1) {
          this.idArray.push({
            id: element.id,
            color:
              'rgb(' + o(r() * s) + ',' + o(r() * s) + ',' + o(r() * s) + ')',
          });
        }
      });

      // this.createActiveArray()

      console.log(' auto einai to id array: ', this.idArray);
    } else {
      let parsedData = JSON.parse(this.dataFromServer);
      this.data = parsedData['data'];

      this.groupedData = this.data;
      console.log('auta einai ta grouparismena data', this.groupedData);
      if (this.groupedData[0].id) {
        let o = Math.round;
        let r = Math.random;
        let s = 255;
        this.data.forEach((element) => {
          let index = this.idArray.findIndex((id) => id.id === element.id);

          if (index === -1) {
            this.idArray.push({
              id: element.id,
              color:
                'rgb(' + o(r() * s) + ',' + o(r() * s) + ',' + o(r() * s) + ')',
            });
          }
        });

        // this.createActiveArray()

        console.log(' auto einai to id array: ', this.idArray);
      }

      this.spatialVisualization();
    }

    // Call api to get noda spatio-temporal data
    this.quoteService
      .getNodaSTData()
      .then((res: any) => {
        // json parse the responce and import in data variable
        let parsedData = JSON.parse(res);
        this.data = parsedData['data'];

        // JS Date needs milli Epoch Timestamp (so below is a milli epoch converter)
        console.log(
          this.data,
          'ayto einai ena date: ' +
            new Date(parseInt(this.timestampManipulation(this.data[0]['time'])))
        );

        this.opt = {
          floor: parseInt(this.timestampManipulation(this.data[0]['time'])),
          ceil: parseInt(
            this.timestampManipulation(this.data[this.data.length - 1]['time'])
          ),
        };

        console.log('floor', this.opt.floor, 'ceil', this.opt.ceil);

        this.value = this.opt.floor;
        this.maxValue =
          this.opt.floor + this.windowBetweenFloorAndCeil * 60 * 60 * 1000;

        this.groupedData = _.groupBy(this.data, 'time');
        console.log('auta einai ta grouparismena data', this.groupedData);

        // for (let key in this.groupedData) {
        //   this.opt.ticksArray.push(parseInt(this.timestampManipulation(key)));
        // }

        // console.log('ticksArray: ', this.opt.ticksArray);

        let o = Math.round;
        let r = Math.random;
        let s = 255;
        this.data.forEach((element) => {
          let index = this.idArray.findIndex((id) => id.id === element.id);

          if (index === -1) {
            this.idArray.push({
              id: element.id,
              color:
                'rgb(' + o(r() * s) + ',' + o(r() * s) + ',' + o(r() * s) + ')',
            });
          }
        });

        // this.createActiveArray()

        console.log(' auto einai to id array: ', this.idArray);
      })
      .catch((err) => {
        console.log(err);
        this.isSpatial = true;
      });

    // Spatial Manipulation
    this.quoteService
      .getNodaSpatialData()
      .then((res: any) => {
        let parsedData = JSON.parse(res);
        this.data = parsedData['data'];

        this.groupedData = this.data;
        console.log('auta einai ta grouparismena data', this.groupedData);
        if (this.groupedData[0].id) {
          let o = Math.round;
          let r = Math.random;
          let s = 255;
          this.data.forEach((element) => {
            let index = this.idArray.findIndex((id) => id.id === element.id);

            if (index === -1) {
              this.idArray.push({
                id: element.id,
                color:
                  'rgb(' +
                  o(r() * s) +
                  ',' +
                  o(r() * s) +
                  ',' +
                  o(r() * s) +
                  ')',
              });
            }
          });

          // this.createActiveArray()

          console.log(' auto einai to id array: ', this.idArray);
        }

        this.spatialVisualization();
      })
      .catch((err) => {
        console.log(err);
        this.isSpatial = false;
      });
  }

  timestampManipulation(time: any) {
    let manipulatedTime;

    if (time.length === 13) {
      manipulatedTime = time;
    } else {
      manipulatedTime = parseInt((time += '000'));
    }

    return manipulatedTime;
  }

  changeFloorOrCeil(type: string) {
    if (type === 'floor') {
      this.changeFloor = !this.changeFloor;
      console.log(this.opt.floor);
    }
    if (type === 'ceil') {
      this.changeCeil = !this.changeCeil;
      console.log(this.opt.ceil);
    }
  }

  onMapReady(map: L.Map) {
    this.map = map;
  }

  getRGBofPin(id: string) {
    const index = this.idArray.findIndex((el) => el.id === id);
    return this.idArray[index].color;
  }

  spatialVisualization() {
    const myRenderer = L.canvas({
      padding: 0.5,
    });

    this.groupedData.forEach((position: any) => {
      const lat = position.lat;
      const lon = position.lon;
      if (position.id) {
        let myIcon = L.divIcon({
          html:
            '<div style="background-color: ' +
            this.getRGBofPin(position.id) +
            ' ; height: 10px; width: 10px; border-radius: 100%;"></div>',
        });

        this.layers.push(
          L.circleMarker([lat, lon], {
            renderer: myRenderer,
            color: this.getRGBofPin(position.id),
            fillColor: this.getRGBofPin(position.id),
            fill: true,
            stroke: false,
            fillOpacity: 1,
          })
          // .bindPopup(
          //   `<div>CraftID: ` +
          //     craftID +
          //     `</div>` +
          //     `<div>TimeStamp: ` +
          //     TimeStamp +
          //     `</div>` +
          //     `<div>Speed: ` +
          //     Speed +
          //     `</div>`
          // )
        );
      } else {
        let myIcon = L.divIcon({
          html:
            '<div style="background-color: green; height: 10px; width: 10px; border-radius: 100%;"></div>',
        });

        this.layers.push(
          L.circleMarker([lat, lon], {
            renderer: myRenderer,
            color: '#228B22',
            fillColor: '#228B22',
            fill: true,
            stroke: false,
            fillOpacity: 1,
          })
          // .bindPopup(
          //   `<div>CraftID: ` +
          //     craftID +
          //     `</div>` +
          //     `<div>TimeStamp: ` +
          //     TimeStamp +
          //     `</div>` +
          //     `<div>Speed: ` +
          //     Speed +
          //     `</div>`
          // )
        );

        this.layers.push(
          L.circleMarker([lat, lon], {
            renderer: myRenderer,
            color: '#228B22',
            fillColor: '#228B22',
            fill: true,
            stroke: false,
            fillOpacity: 1,
          })
          // .bindPopup(
          //   `<div>CraftID: ` +
          //     craftID +
          //     `</div>` +
          //     `<div>TimeStamp: ` +
          //     TimeStamp +
          //     `</div>` +
          //     `<div>Speed: ` +
          //     Speed +
          //     `</div>`
          // )
        );
      }
    });
  }

  playSpatioTemporal() {
    // this.value = this.opt.floor;
    this.maxValue =
      this.opt.floor + this.windowBetweenFloorAndCeil * 60 * 60 * 1000;
    let i = 0;

    // for (let key in this.groupedData) {
    for (let key in this.groupedData) {
      console.log('eimai akrivos apekso!');

      i++;
      setTimeout(() => {
        if (parseInt(this.timestampManipulation(key)) > this.opt.floor) {
          this.groupedData[key].forEach((element: any) => {
            const lat = element.lat;
            const lon = element.lon;
            const time = element.time;
            let myIcon = L.divIcon({
              html:
                '<div style="background-color: ' +
                this.getRGBofPin(element.id) +
                ' ; height: 10px; width: 10px; border-radius: 100%;"></div>',
            });

            // this.layers = [];
            const myRenderer = L.canvas({
              padding: 0.5,
            });

            this.layers.push(
              L.circleMarker([lat, lon], {
                renderer: myRenderer,
                color: this.getRGBofPin(element.id),
                fillColor: this.getRGBofPin(element.id),
                fill: true,
                stroke: false,
                fillOpacity: 1,
              })
              // L.marker([lat, lon], {
              //   icon: myIcon,
              // })
              // .bindPopup(
              //   `<div>CraftID: ` +
              //     craftID +
              //     `</div>` +
              //     `<div>TimeStamp: ` +
              //     TimeStamp +
              //     `</div>` +
              //     `<div>Speed: ` +
              //     Speed +
              //     `</div>`
              // )
            );
            // this.value = parseInt(this.timestampManipulation(time));
            // this.maxValue = this.value + this.windowBetweenFloorAndCeil * 60 * 60 * 1000;
            // this.map.panTo(new L.LatLng(lat, lon));
          });
        }
        this.value = parseInt(this.timestampManipulation(key));
        this.maxValue =
          this.value + this.windowBetweenFloorAndCeil * 60 * 60 * 1000;

        if (this.layers.length >= 80) {
          this.layers.splice(0, 30);
        }
        // console.log("auta einai ta layers", this.layers);
      }, i * (this.fps * 1000));
    }
  }
}
