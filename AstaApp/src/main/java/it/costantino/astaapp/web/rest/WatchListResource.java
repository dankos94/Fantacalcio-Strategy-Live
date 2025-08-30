package it.costantino.astaapp.web.rest;

import it.costantino.astaapp.repository.WatchListRepository;
import it.costantino.astaapp.service.WatchListService;
import it.costantino.astaapp.service.dto.WatchListDTO;
import it.costantino.astaapp.web.rest.errors.BadRequestAlertException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tech.jhipster.web.util.HeaderUtil;
import tech.jhipster.web.util.ResponseUtil;

/**
 * REST controller for managing {@link it.costantino.astaapp.domain.WatchList}.
 */
@RestController
@RequestMapping("/api/watch-lists")
public class WatchListResource {

    private static final Logger LOG = LoggerFactory.getLogger(WatchListResource.class);

    private static final String ENTITY_NAME = "watchList";

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final WatchListService watchListService;

    private final WatchListRepository watchListRepository;

    public WatchListResource(WatchListService watchListService, WatchListRepository watchListRepository) {
        this.watchListService = watchListService;
        this.watchListRepository = watchListRepository;
    }

    /**
     * {@code POST  /watch-lists} : Create a new watchList.
     *
     * @param watchListDTO the watchListDTO to create.
     * @return the {@link ResponseEntity} with status {@code 201 (Created)} and with body the new watchListDTO, or with status {@code 400 (Bad Request)} if the watchList has already an ID.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PostMapping("")
    public ResponseEntity<WatchListDTO> createWatchList(@RequestBody WatchListDTO watchListDTO) throws URISyntaxException {
        LOG.debug("REST request to save WatchList : {}", watchListDTO);
        if (watchListDTO.getId() != null) {
            throw new BadRequestAlertException("A new watchList cannot already have an ID", ENTITY_NAME, "idexists");
        }
        watchListDTO = watchListService.save(watchListDTO);
        return ResponseEntity.created(new URI("/api/watch-lists/" + watchListDTO.getId()))
            .headers(HeaderUtil.createEntityCreationAlert(applicationName, false, ENTITY_NAME, watchListDTO.getId().toString()))
            .body(watchListDTO);
    }

    /**
     * {@code PUT  /watch-lists/:id} : Updates an existing watchList.
     *
     * @param id the id of the watchListDTO to save.
     * @param watchListDTO the watchListDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated watchListDTO,
     * or with status {@code 400 (Bad Request)} if the watchListDTO is not valid,
     * or with status {@code 500 (Internal Server Error)} if the watchListDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PutMapping("/{id}")
    public ResponseEntity<WatchListDTO> updateWatchList(
        @PathVariable(value = "id", required = false) final Long id,
        @RequestBody WatchListDTO watchListDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to update WatchList : {}, {}", id, watchListDTO);
        if (watchListDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, watchListDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!watchListRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        watchListDTO = watchListService.update(watchListDTO);
        return ResponseEntity.ok()
            .headers(HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, watchListDTO.getId().toString()))
            .body(watchListDTO);
    }

    /**
     * {@code PATCH  /watch-lists/:id} : Partial updates given fields of an existing watchList, field will ignore if it is null
     *
     * @param id the id of the watchListDTO to save.
     * @param watchListDTO the watchListDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated watchListDTO,
     * or with status {@code 400 (Bad Request)} if the watchListDTO is not valid,
     * or with status {@code 404 (Not Found)} if the watchListDTO is not found,
     * or with status {@code 500 (Internal Server Error)} if the watchListDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PatchMapping(value = "/{id}", consumes = { "application/json", "application/merge-patch+json" })
    public ResponseEntity<WatchListDTO> partialUpdateWatchList(
        @PathVariable(value = "id", required = false) final Long id,
        @RequestBody WatchListDTO watchListDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to partial update WatchList partially : {}, {}", id, watchListDTO);
        if (watchListDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, watchListDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!watchListRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        Optional<WatchListDTO> result = watchListService.partialUpdate(watchListDTO);

        return ResponseUtil.wrapOrNotFound(
            result,
            HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, watchListDTO.getId().toString())
        );
    }

    /**
     * {@code GET  /watch-lists} : get all the watchLists.
     *
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and the list of watchLists in body.
     */
    @GetMapping("")
    public List<WatchListDTO> getAllWatchLists() {
        LOG.debug("REST request to get all WatchLists");
        return watchListService.findAll();
    }

    /**
     * {@code GET  /watch-lists/:id} : get the "id" watchList.
     *
     * @param id the id of the watchListDTO to retrieve.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the watchListDTO, or with status {@code 404 (Not Found)}.
     */
    @GetMapping("/{id}")
    public ResponseEntity<WatchListDTO> getWatchList(@PathVariable("id") Long id) {
        LOG.debug("REST request to get WatchList : {}", id);
        Optional<WatchListDTO> watchListDTO = watchListService.findOne(id);
        return ResponseUtil.wrapOrNotFound(watchListDTO);
    }

    /**
     * {@code DELETE  /watch-lists/:id} : delete the "id" watchList.
     *
     * @param id the id of the watchListDTO to delete.
     * @return the {@link ResponseEntity} with status {@code 204 (NO_CONTENT)}.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteWatchList(@PathVariable("id") Long id) {
        LOG.debug("REST request to delete WatchList : {}", id);
        watchListService.delete(id);
        return ResponseEntity.noContent()
            .headers(HeaderUtil.createEntityDeletionAlert(applicationName, false, ENTITY_NAME, id.toString()))
            .build();
    }
}
