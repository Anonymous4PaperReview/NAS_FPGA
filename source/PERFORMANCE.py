import math
import networkx as nx
# import matplotlib.pyplot as plt
import sys
import random
import os
import sys
import math
from math import *
import csv
import time
import copy 
from PARA import DESIGN_PARA
class SCHEDULE:
    def __init__(self,schedule_in, S1, G, OPT,NUM_LAYERS):
        self.schedule_in = schedule_in
        self.S1 = S1
        self.G = G
        self.timeline={}
        self.TotalTime = 0;
        self.tasknodes_list=[]
        self.OPT=OPT
        self.NUM_LAYERS=NUM_LAYERS
        self.time_point = []

    '''function to build graph'''
    def ofmtm(self,layer):
        return layer[5]
    def ifmtn(self,layer,layers):
        layer_index=layers.index(layer)
        layer_next=layers[layer_index+1]
        return layer_next[6]

    def ofmchannel(self,layer):
        return math.ceil(layer[1]/layer[5])
        '''返回ofm 被Tm分割的个数 '''
    def ifmchannel(self,layer,layers):
        layer_index=layers.index(layer)
        layer_next=layers[layer_index+1]
        layer_next_Tn=layer_next[6]
        return math.ceil(layer[1]/layer_next_Tn)
        '''返回ifm被Tn分割的个数'''
    def rc(self,layer):
        return math.ceil(layer[3]/layer[7])*math.ceil(layer[4]/layer[8])
        '''返回rc分割的个数'''
    def layername(self,layer):
        return layer[0]
        '''返回rc分割的个数'''
    def firetime(self,layer):
        return layer[-1]*layer[-1]*layer[-2]*layer[-3]
    '''end function to build graph'''

    '''function to do S1 scheduel'''
    def ifmchannel_task(self,layer):
        return math.ceil(layer[2]/layer[6])

    def builder(self,layers):
        self.G.clear()

        '''data node'''
        for i in range(0,len(layers)): 
            obj_layer=layers[i]
            '''first layer is input, only consider ofm'''
            if i==0:
                ifm_channel=self.ifmchannel(obj_layer,layers)
                rc_par=self.rc(obj_layer)
                for j in range(1,ifm_channel+1):
                    for k in range(1,rc_par+1):
                        node_name="ifm_" + obj_layer[0] + "_"  + str(j) + "_" + str(k)
                        self.G.add_node(node_name, layerid=obj_layer[0], type="ifm", channel=j, prc=k, status="Yes")

            '''中间层包含ifm, ofm'''
            '''生成ifm node, 作为下一层的ifm'''
            if i< (len(layers)-1) and i>0:
                ifm_channel=self.ifmchannel(obj_layer,layers)
                rc_par=self.rc(obj_layer)
                # print("ifm",obj_layer,ifm_channel,rc_par)
                for j in range(1,ifm_channel+1):
                    for k in range(1,rc_par+1):
                        node_name="ifm_" + obj_layer[0]  + "_"+ str(j) + "_" + str(k)
                        self.G.add_node(node_name, layerid=obj_layer[0], type="ifm", channel=j, prc=k, status="No")

            # '''生成ofm node,作为上一层的ofm'''
                ofm_channel=self.ofmchannel(obj_layer)
                
                rc_par=self.rc(obj_layer)
                # print("ofm",obj_layer,ofm_channel,rc_par)
                for j in range(1,ofm_channel+1):
                    for k in range(1,rc_par+1):
                        node_name="ofm_" + obj_layer[0] + "_"+  str(j) + "_" + str(k)
                        self.G.add_node(node_name, layerid=obj_layer[0], type="ofm", channel=j, prc=k, status="No")
            if i== (len(layers)-1):
                ofm_channel=self.ofmchannel(obj_layer)
                rc_par=self.rc(obj_layer)
                # print("ofm",obj_layer,ofm_channel,rc_par)
                for j in range(1,ofm_channel+1):
                    for k in range(1,rc_par+1):
                        node_name="ofm_" + obj_layer[0] + "_"+  str(j) + "_" + str(k)
                        self.G.add_node(node_name, layerid=obj_layer[0], type="ofm", channel=j, prc=k, status="No")

        # print(self.G.nodes())
        '''task node and edges connected to task node'''
        '''从ofm开始考虑'''
        for i in range(1,len(layers)):

            '''当前layer ofm'''
            obj_layer=layers[i]
            obj_layer_name=obj_layer[0]
            layer_ofm=[]

            '''当前layer的前一层'''
            layer_pred=layers[i-1]
            layer_pred_name=layer_pred[0]
            layer_pred_ifm=[]
            firetime_=self.firetime(layers[i])
            # print(layer_pred_name)
            '''开始建立对应关系'''
            '''ofm'''
            for x,y in self.G.nodes(data=True):
                if y['layerid']==obj_layer_name and y['type']=="ofm":
                    layer_ofm.append(x)
            '''ifm'''
            for x,y in self.G.nodes(data=True):
                if y['layerid']==layer_pred_name and y['type']=="ifm":
                    layer_pred_ifm.append(x)

            
            # '''遍历ofm中每一个分块, 找对对应上一层ifm分块，建立task node, task node和上层ifm映射'''
            for x in layer_ofm:
                ofmchannel_=self.G.node[x]['channel']
                ofmprc=self.G.node[x]['prc']
                for y in layer_pred_ifm:
                    if self.G.node[y]['prc']==ofmprc:
                        ifmchannel_=self.G.node[y]['channel']
                        # node_name = str(y) + '||' + str(x)
                        # layerid_temp=str(y) + '&' + str(x)
                        node_name = "V" + str(i) + "_" + str(ifmchannel_) + "_" + str(ofmchannel_) + "_" + str(ofmprc)
                        # print(layerid_temp, obj_layer_name)
                        self.G.add_node(node_name, layerid= obj_layer_name+'_task', type='task', ifm_channel= ifmchannel_, ifm_rc=ofmprc, ofm_channel=ofmchannel_, ofm_rc=ofmprc, status="null",  time =firetime_, priority=0) # data=true那么必须有layerid这个东西
                        '''last layer ifm to task noe'''
                        self.G.add_edge(y,node_name)
                        '''task node to ofm'''
                        self.G.add_edge(node_name,x)
            # if i==3:
            #     print(self.G.pred['V3_1_1_1'])
            # if i==1:
            #     print(self.G.pred['V1_1_1_1'])


        '''layer中 ifm ofm edge 关系'''
        # print(len(layers))
        for i in range(0, len(layers)):
            if i < len(layers)-1 and i > 0:
                obj_ifmchannel=self.ifmchannel(layers[i],layers)
                obj_ofmchannel=self.ofmchannel(layers[i])
                obj_tm=self.ofmtm(layers[i])
                obj_tn=self.ifmtn(layers[i],layers)
                obj_rc=self.rc(layers[i])
                obj_name=self.layername(layers[i])
                # print(obj_ifmchannel,obj_ofmchannel,obj_tm,obj_tn,obj_rc,obj_name)
                for j in range (1,obj_rc+1):
                    ofm_start=1                   
                    for k in range (1,obj_ifmchannel+1):
                        # print(" ifm new channel", k, ofm_start)
                        ofm_start_add= ofm_start 
                        while True:
                        # for l in range(ofm_start,obj_ofmchannel+1):
                            if ofm_start_add*obj_tm==k*obj_tn:
                                for x,y in self.G.nodes(data=True):
                                    if y['layerid']==obj_name and y['type']=="ifm" and y['channel']==k  and y['prc']==j:
                                        sink_node=x
                                        # print(sink_node, '\n')
                                for m in range(ofm_start,ofm_start_add+1):
                                    for x,y in self.G.nodes(data=True):
                                        if y['layerid']==obj_name and y['type']=="ofm" and y['channel']==m  and y['prc']==j:
                                            source_node=x
                                            # print(source_node)
                                            self.G.add_edge(source_node,sink_node)
                                ofm_start=ofm_start_add+1
                                break
                           
                            if ofm_start_add*obj_tm > k*obj_tn:
                                for x,y in self.G.nodes(data=True):
                                    if y['layerid']==obj_name and y['type']=="ifm" and y['channel']==k  and y['prc']==j:
                                        sink_node=x
                                        # print(sink_node, '\n')
                                for m in range(ofm_start,ofm_start_add+1):
                                    for x,y in self.G.nodes(data=True):
                                        if y['layerid']==obj_name and y['type']=="ofm" and y['channel']==m  and y['prc']==j:
                                            source_node=x
                                            # print(source_node)
                                            self.G.add_edge(source_node,sink_node)
                                ofm_start=ofm_start_add
                                break
                            
                            ofm_start_add=ofm_start_add+1
        return self.G

                          


    '''每次初始化一下 确保0_indegree的data node 被删除'''
    def clean_data_node(self):
        to_remove=[]
        for x,y in self.G.nodes(data=True):
            if y['type']=="ifm" or y['type']=='ofm':
                # print(x)
                # print(G.pred[x], '\n')
                if not self.G.pred[x]:
                    # print(x)
                    # print("empty")
                    to_remove.append(x)
                    # G.remove_node(x)
        for i in range(len(to_remove)):
            self.G.remove_node(to_remove[i])
        return self.G

    # G = builder(G)
    # print(G.nodes())
    # G=clean_data_node(G)
    # print(G.pred['c2_ifm_2_1||c3_ofm_15_1'])

    '''寻找一个tasklyer priority最大的一个'''

    def find_task_node(self, task_layerid):
        pick_node=0
        obj_node=[]
        priority_list=[]
        for x,y in self.G.nodes(data=True):
            if y['type']=='task' and y['layerid']==task_layerid:
                if not self.G.pred[x]:
                    # print(x)
                    obj_node.append(x)
        if not obj_node:
            # print('please wait')
            return False
        for i in range(len(obj_node)):
            priority_list.append(self.G.node[obj_node[i]]['priority'])
        MAX=max(priority_list)
        count=priority_list.count(MAX)
        # print(obj_node,'\n')
        if count==1:
            pick_node = obj_node[priority_list.index(MAX)]
        if count>1:
            pick_nodes = [i for i,v in enumerate(priority_list) if v==MAX]
            # print("===============================",len(pick_nodes))
            # opt_node = 0
            # opt_out = 0        
            # # print(pick_nodes)
            # for n in pick_nodes:
            #     if G.out_degree(obj_node[n])>opt_val:
            #         opt_val = G.out_degree(obj_node[n])
            #         opt_node = n
            # pick_node=obj_node[opt_node]
            pick_node=obj_node[pick_nodes[random.randint(0,len(pick_nodes)-1)]]
            # pick_node=obj_node[pick_nodes[-1]]

            # sucess_nodeeee=G.succ[pick_node]
            # for x in sucess_nodeeee:
            #     G_count+= len(G.succ[x])
        return pick_node
        

    # print("ppp",(find_task_node(G, "c2_task")))


    def task_layer(self, task_layername):
        picked_node=self.find_task_node(task_layername)
        if picked_node== False:
            return "null"
        picked_success = self.G.succ[picked_node] # 子节点只有一个

        self.G.remove_node(picked_node)

        # print(picked_node,"picked_node")
        # print(picked_success, "picked_successsssssssssssssssssssssssssssssssssssssssssssssssssssssssssss")
        for x in picked_success: #因为数据结构必须这样操作， 其实picked_success只有一个值
            success_pred=self.G.pred[x] #但是返回的可能是多个值
            success_success = self.G.succ[x]
        if success_pred:
            # print("line 1 2")
            for x in success_pred: #遍历每一个preds
                if (self.G.node[x]['type'] == "task"):      #对应伪代码中line1
                    if x != picked_node:
                        temp_priority=self.G.node[x]['priority']+1
                        self.G.node[x]['priority']=temp_priority
                # elif G.node[x]['type']=="ifm" or G.node[x]['type']=="ofm": #'''对应伪代码中line2'''
                    # print("go into line 2++++++++++++++++++")
                    # success_pred_pred=G.pred[x]
                    # for y in success_pred_pred:
                    #     if y != picked_node:
                    #         temp_priority=G.node[y]['priority']+1
                    #         G.node[y]['priority']=temp_priority
        if success_success:
            for x in success_success:
                y_list=self.G.pred[x]
                for y in y_list:
                    z_list = self.G.pred[y]
                    for z in z_list:
                        self.G.node[z]['priority']+=1

        if not success_pred:
            # print("line3")
            for x in picked_success: #因为数据结构必须这样操作， 其实picked_success只有一个值
                picked_success_success=self.G.succ[x]
            for x in picked_success:
                self.G.remove_node(x)
            for x in picked_success_success: # 可能会有多个儿子success，遍历每一个
                back_ofm=self.G.pred[x] #也有可能有多个
                # if not back_ofm:
                #     G.remove_node(x)
                for y in back_ofm:
                    back_ofm_task=self.G.pred[y]
                    for z in back_ofm_task:
                        temp_priority=self.G.node[z]['priority']+1
                        self.G.node[z]['priority']=temp_priority
        return picked_node


    def find_task_node_sequential(self, layer_select ):
        target_task_layer=self.tasknodes_list[layer_select]
        for i in range(len(target_task_layer)):
            if target_task_layer[i] ==0 and i <len(target_task_layer)-1:
                continue
            if target_task_layer[i] !=0:
                pick_nodeee=target_task_layer[i]
                return pick_nodeee
            if target_task_layer[i] ==0 and i==len(target_task_layer)-1:
                return False
        # return pick_nodeee

    def task_layer_sequential(self, layer_select):
        picked_node=self.find_task_node_sequential(layer_select)
        if picked_node== False:
            return "end",0
        # print(picked_node)
        # print(G.pred[picked_node])
        # print(picked_node)
        # print("debug")
        if not self.G.pred[picked_node]:
            picked_success = self.G.succ[picked_node] # 子节点只有一个

            node_time=self.G.node[picked_node]['time']
            self.G.remove_node(picked_node)

            # print(picked_node,"picked_node")
            # print(picked_success, "picked_successsssssssssssssssssssssssssssssssssssssssssssssssssssssssssss")
            for x in picked_success: #因为数据结构必须这样操作， 其实picked_success只有一个值
                success_pred=self.G.pred[x] #但是返回的可能是多个值
            if not success_pred:
                for x in picked_success:
                    self.G.remove_node(x)
            else:
                return picked_node, node_time
            return picked_node, node_time

        elif self.G.pred[picked_node] and self.OPT==1:
            target_task_layer=self.tasknodes_list[layer_select]
            for i in range(len(target_task_layer)):
                if target_task_layer[i] ==0:
                    continue
                elif target_task_layer[i] !=0:
                    picked_node=target_task_layer[i]
                    if not self.G.pred[picked_node]:
                        picked_success = self.G.succ[picked_node]
                        node_time=self.G.node[picked_node]['time']
                        self.G.remove_node(picked_node)                   
                        for x in picked_success: 
                            success_pred=self.G.pred[x] 
                        if not success_pred:
                            for x in picked_success:
                                self.G.remove_node(x)
                        else:
                            return picked_node,node_time
                        return picked_node, nodetime
        
        return False, 0
        


    # self.timeline={}
    # self.TotalTime = 0;
    # self.tasknodes_list=[]

    def schedule_run(self,layers,layersname):
        self.timeline={}
        self.G = self.builder(layers)

        tasklayers=len(layers)-1
        k=0
        # G=clean_data_node(Graph)
        
        while self.schedule_in==1:
            k=k+1
            # print(k)
            execute=[]
            '''对于每一层tasklayer都同时考虑'''
            self.G=self.clean_data_node()
            for i in range(1,len(layersname)):
                # print("+++++++++++++++++++++++++++++++++++++++","this is task layer", i)
                task_layerid=layersname[i]
                task_layerid= task_layerid + "_task"
                # print(i,task_layerid)       
                picked_task_at_k = self.task_layer(task_layerid)
                if picked_task_at_k == "null":                
                    continue
                else:
                    self.timeline[(k,i)]=picked_task_at_k
                    execute.append(picked_task_at_k)
            # print(k,len(execute),execute)
            if len((self.G.edges()))==0 or k>10000:
                # print(k)
                self.TotalTime = k
                break
        
        while self.schedule_in==2:
            self.G=self.clean_data_node()
            self.tasknodes_list = []
            for p in range(1,len(layersname)):
                task_layer_obj=layers[p]
                task_layerid=layersname[p]
                task_layerid= task_layerid + "_task"
                tasknodes=[]
                tasknodes_sorted=[]
                for x,y in self.G.nodes(data=True):
                    if y['layerid']==task_layerid:
                        tasknodes.append(x)
                
                for j in range(1,self.rc(task_layer_obj)+1):
                    if self.OPT==1:
                        if p%2==1:
                            for k in range(1,self.ofmchannel(task_layer_obj)+1):        
                                for l in range (1,self.ifmchannel_task(task_layer_obj)+1):
                                    for candidate in tasknodes:
                                        if self.G.node[candidate]['ofm_rc']==j and self.G.node[candidate]['ofm_channel']==k and self.G.node[candidate]['ifm_channel']==l:
                                            # print("yes")
                                            # print(candidate)
                                            tasknodes_sorted.append(candidate)                                          
                                            # print(candidate,"haha",self.G.node[candidate]['ifm_channel'],self.G.node[candidate]['ofm_channel'])

                        else:
                            for l in range (1,self.ifmchannel_task(task_layer_obj)+1):
                                for k in range(1,self.ofmchannel(task_layer_obj)+1):
                                    for candidate in tasknodes:
                                        if self.G.node[candidate]['ofm_rc']==j and self.G.node[candidate]['ofm_channel']==k and self.G.node[candidate]['ifm_channel']==l:
                                            tasknodes_sorted.append(candidate)
                                            # print(candidate,"haha",self.G.node[candidate]['ifm_channel'],self.G.node[candidate]['ofm_channel'])

                    elif self.OPT==0:
                        for k in range(1,self.ofmchannel(task_layer_obj)+1):
                            for l in range (1,self.ifmchannel_task(task_layer_obj)+1):                                
                                    for candidate in tasknodes:
                                        if self.G.node[candidate]['ofm_rc']==j and self.G.node[candidate]['ofm_channel']==k and self.G.node[candidate]['ifm_channel']==l:
                                            # print("yes")
                                            # print(candidate)
                                            tasknodes_sorted.append(candidate)
                                            # print(candidate,"hahaaa")
                                            # tasknodes.append(candidate)
                # if p==1:
                #     print(tasknodes_sorted)
                self.tasknodes_list.append(tasknodes_sorted)                       

            k=0
            MAX=sys.maxsize
            schedule_time=[MAX]*self.NUM_LAYERS
            schedule_status=[0]*self.NUM_LAYERS
            loop_count = 0

            while self.S1==1:
                self.time_point.append(k)
                loop_count+=1
                self.G=self.clean_data_node()
                for i in range(len(self.tasknodes_list)):

                    if schedule_status[i]==1:
                        continue
                    picked_task_at_k, node_time = self.task_layer_sequential(i)
                    if picked_task_at_k == False or picked_task_at_k == "end":  
                        schedule_time[i]=MAX
                        schedule_status[i]=0
                        continue
                    else:
                        self.timeline[(k,i)]=picked_task_at_k
                        index_target=self.tasknodes_list[i].index(picked_task_at_k)
                        self.tasknodes_list[i][index_target]=0     
                        schedule_time[i]= k + node_time
                        schedule_status[i]=1
                


                k=min(schedule_time)
                idxs = [iii for iii,v in enumerate(schedule_time) if v==k]

                # print(loop_count,k,idxs,self.timeline.keys())
                # if loop_count>10:
                #     sys.exit(0)
                for idx in idxs:
                    schedule_time[idx]=MAX
                    schedule_status[idx]=0

                if len((self.G.edges()))==0:                    
                    self.TotalTime = k
                    break
            break
        print(len(self.G.edges()))
        print("done~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        if self.schedule_in==1:
            for time in range(1,self.TotalTime+1):
                for i in range(1,len(layersname)) :
                    if (time,i) in self.timeline.keys():
                        print (time,i,self.timeline[(time,i)])
                      
        '''elif self.schedule_in==2:
            for time in self.time_point:
                print(time,end=" ")
                for i in range(len(self.tasknodes_list)):        
                    if (time,i) in self.timeline.keys():                
                        print (self.timeline[(time,i)],"|", end =" ")                            
                    else:
                        print("________","|",end =" ")
                print()
        '''
        return k
